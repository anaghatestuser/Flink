/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc
import org.apache.flink.table.planner.utils.ShortcutUtils

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexNode, RexShuttle}

import scala.collection.JavaConversions._

/**
 * Rule that eliminates common Python UDF sub-expressions between the condition and projection of a
 * Calc node.
 *
 * <p>After [[RemoteCalcSplitConditionRule]] splits a Calc with Python UDFs in its condition, the
 * result is a two-level Calc structure:
 *
 * <pre> TopCalc(projection=[pyFunc(a, b) + 1, pyFunc(a, b) + 2], condition=[$2 > 0])
 * BottomCalc(projection=[a, b, pyFunc(a, b) AS f0]) </pre>
 *
 * <p>The TopCalc's projection still contains `pyFunc(a, b)` which is structurally identical to the
 * already-computed `f0` in the BottomCalc. This rule detects such duplicates and rewrites the
 * TopCalc's projection to reference the BottomCalc's output directly:
 *
 * <pre> TopCalc(projection=[$2 + 1, $2 + 2], condition=[$2 > 0]) BottomCalc(projection=[a, b,
 * pyFunc(a, b) AS f0]) </pre>
 *
 * <p>This ensures that when the plan is later split into separate PythonCalc nodes, the same UDF
 * call is not computed twice across condition and projection.
 *
 * @param callFinder
 *   the finder used to identify remote (Python) function calls
 */
class RemoteCalcConditionProjectionCseRule(private val callFinder: RemoteCallFinder)
  extends RelOptRule(
    operand(classOf[FlinkLogicalCalc], operand(classOf[FlinkLogicalCalc], any)),
    "RemoteCalcConditionProjectionCseRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val topCalc: FlinkLogicalCalc = call.rel(0)
    val bottomCalc: FlinkLogicalCalc = call.rel(1)

    // This rule only applies when the top calc has a condition — it deduplicates Python UDF
    // calls shared between condition and projection. Without a condition, there is nothing
    // to deduplicate, and firing would interfere with other rules (e.g. PythonMapMergeRule).
    if (topCalc.getProgram.getCondition == null) {
      return false
    }

    val topProjects = topCalc.getProgram.getProjectList.map(topCalc.getProgram.expandLocalRef)

    // The bottom calc must have deterministic Python UDF calls in its projection
    // (from condition extraction). Non-deterministic calls must NOT be shared.
    val bottomProjects =
      bottomCalc.getProgram.getProjectList.map(bottomCalc.getProgram.expandLocalRef)
    val bottomPythonCalls: Map[RexNode, Int] = bottomProjects.zipWithIndex
      .filter {
        case (node, _) =>
          callFinder.containsRemoteCall(node) &&
          ShortcutUtils.isDeterministicThroughProgram(node, null)
      }
      .map { case (node, idx) => (node, idx) }
      .toMap

    if (bottomPythonCalls.isEmpty) {
      return false
    }

    // The top calc's projection must contain Python UDF calls that are structurally equal
    // to calls already computed in the bottom calc's projection
    topProjects.exists(containsCallMatchingBottom(_, bottomPythonCalls))
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val topCalc: FlinkLogicalCalc = call.rel(0)
    val bottomCalc: FlinkLogicalCalc = call.rel(1)
    val rexBuilder: RexBuilder = call.builder().getRexBuilder

    val topProjects = topCalc.getProgram.getProjectList.map(topCalc.getProgram.expandLocalRef)
    val topCondition = Option(topCalc.getProgram.getCondition)
      .map(topCalc.getProgram.expandLocalRef)

    val bottomProjects =
      bottomCalc.getProgram.getProjectList.map(bottomCalc.getProgram.expandLocalRef)

    // Build a map from deterministic Python UDF call (in bottom calc's projection) to its
    // output index. Non-deterministic calls are excluded to prevent incorrect sharing.
    val bottomPythonCalls: Map[RexNode, Int] = bottomProjects.zipWithIndex
      .filter {
        case (node, _) =>
          callFinder.containsRemoteCall(node) &&
          ShortcutUtils.isDeterministicThroughProgram(node, null)
      }
      .map { case (node, idx) => (node, idx) }
      .toMap

    // Rewrite the top calc's projection: replace Python UDF calls that match bottom calc's
    // projection with RexInputRef pointing to the bottom calc's output position
    val rewriter = new CseRewriteShuttle(bottomPythonCalls, bottomCalc.getRowType)
    val newTopProjects: Seq[RexNode] = topProjects.map(_.accept(rewriter))

    // Check if any rewriting actually happened
    if (!rewriter.hasRewritten) {
      return
    }

    // Build the new top calc with rewritten projections
    val newTopCalc = topCalc.copy(
      topCalc.getTraitSet,
      bottomCalc,
      org.apache.calcite.rex.RexProgram.create(
        bottomCalc.getRowType,
        newTopProjects.toList,
        topCondition.orNull,
        topCalc.getRowType,
        rexBuilder
      )
    )

    call.transformTo(newTopCalc)
  }

  /**
   * Checks if a RexNode contains a Python UDF call that is structurally equal to one of the calls
   * in the bottom calc's projection.
   */
  private def containsCallMatchingBottom(
      node: RexNode,
      bottomPythonCalls: Map[RexNode, Int]): Boolean = {
    node match {
      case call: RexCall =>
        if (callFinder.isRemoteCall(call) && bottomPythonCalls.contains(call)) {
          true
        } else {
          call.getOperands.exists(containsCallMatchingBottom(_, bottomPythonCalls))
        }
      case _ => false
    }
  }

  // Consider the rules to be equal if they are the same class and their call finders are the same
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: RemoteCalcConditionProjectionCseRule =>
        super.equals(other) && callFinder.getClass.equals(other.callFinder.getClass)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    super.hashCode() * 31 + callFinder.getClass.hashCode()
  }
}

/**
 * A RexShuttle that replaces Python UDF calls in the top calc's projection with RexInputRef
 * pointing to the bottom calc's output position where the same call was already computed.
 */
private class CseRewriteShuttle(
    bottomPythonCalls: Map[RexNode, Int],
    bottomRowType: org.apache.calcite.rel.`type`.RelDataType)
  extends RexShuttle {

  private var _hasRewritten: Boolean = false

  def hasRewritten: Boolean = _hasRewritten

  override def visitCall(call: RexCall): RexNode = {
    // If this call matches a bottom calc's Python UDF output, replace with InputRef
    bottomPythonCalls.get(call) match {
      case Some(idx) =>
        _hasRewritten = true
        new RexInputRef(idx, bottomRowType.getFieldList.get(idx).getType)
      case None =>
        // Continue rewriting operands
        super.visitCall(call)
    }
  }
}
