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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.{BeforeEach, Test}

/**
 * Tests for
 * [[org.apache.flink.table.planner.plan.rules.logical.RemoteCalcConditionProjectionCseRule]].
 *
 * Verifies that Python UDF calls shared between condition and projection are deduplicated so that
 * the same UDF is not computed in two separate PythonCalc nodes. Tests cover:
 *   - UDF nested calls
 *   - UDF repeated calls in projection
 *   - UDF in condition (WHERE clause)
 *
 * Each test verifies the final physical execution plan (ExecNode level) to confirm the number of
 * PythonCalc nodes and UDF call count is minimized.
 */
class PythonCalcConditionCseTest extends TableTestBase {

  private val util = streamTestUtil()

  @BeforeEach
  def setup(): Unit = {
    util.addTableSource[(Int, Int, Int)]("MyTable", 'a, 'b, 'c)
    util.addTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"))
    util.addTemporarySystemFunction("pyFunc2", new PythonScalarFunction("pyFunc2"))
    util.addTemporarySystemFunction("pyFunc3", new PythonScalarFunction("pyFunc3"))
    util.addTemporarySystemFunction("pyFunc4", new BooleanPythonScalarFunction("pyFunc4"))
  }

  @Test
  def testSameUdfInConditionAndProjection(): Unit = {
    // pyFunc1(a, b) appears in both WHERE and SELECT — should result in only one PythonCalc node
    val sqlQuery =
      "SELECT pyFunc1(a, b) + 1, pyFunc1(a, b) + 2 FROM MyTable WHERE pyFunc1(a, b) > 0"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testDifferentUdfInConditionAndProjection(): Unit = {
    // Different UDFs in condition and projection — two PythonCalc nodes expected
    val sqlQuery = "SELECT pyFunc1(a, b) FROM MyTable WHERE pyFunc2(a, c) > 0"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNestedUdfInProjectionWithSameInCondition(): Unit = {
    // pyFunc1(a, b) is used in condition and also nested inside pyFunc2 in projection
    val sqlQuery =
      "SELECT pyFunc2(pyFunc1(a, b), c), pyFunc1(a, b) FROM MyTable WHERE pyFunc1(a, b) > 0"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRepeatedUdfCallsInProjectionWithCondition(): Unit = {
    // pyFunc1(a, b) repeated 3 times in projection and once in condition — single PythonCalc
    val sqlQuery =
      """SELECT pyFunc1(a, b) + 1, pyFunc1(a, b) + 2, pyFunc1(a, b) + 3
        |FROM MyTable
        |WHERE pyFunc1(a, b) > 0""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testConditionOnlyUdf(): Unit = {
    // UDF only in condition, not in projection — standard split behavior
    val sqlQuery = "SELECT a, b FROM MyTable WHERE pyFunc4(a, c)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultipleUdfsSharedBetweenConditionAndProjection(): Unit = {
    // Two different UDFs both appear in condition and projection — single PythonCalc
    val sqlQuery =
      """SELECT pyFunc1(a, b) + pyFunc2(b, c)
        |FROM MyTable
        |WHERE pyFunc1(a, b) > 0 AND pyFunc2(b, c) > 0""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNestedUdfWithCseAnnotation(): Unit = {
    // Deeply nested UDF calls: pyFunc1(a, b) is shared across multiple levels.
    // After condition-projection CSE rule extracts pyFunc1(a, b) as f0,
    // the second PythonCalc will have multiple UDF calls with nested references,
    // triggering the CSE annotation in getDescription().
    val sqlQuery =
      """SELECT pyFunc1(a, b), pyFunc1(pyFunc1(a, b), c), pyFunc1(pyFunc1(pyFunc1(a, b), c), a)
        |FROM MyTable
        |WHERE pyFunc1(a, b) > 0""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }
}
