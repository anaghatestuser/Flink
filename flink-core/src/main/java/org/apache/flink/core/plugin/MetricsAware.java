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

package org.apache.flink.core.plugin;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;

/**
 * Capability marker for {@link Plugin}s that want to register Flink metrics.
 *
 * <p>This is an opt-in extension to the plugin SPI. A plugin declares {@code implements
 * SomePluginSpi, MetricsAware}; plugins that do not implement it are byte-for-byte unchanged and
 * emit no metrics.
 *
 * <p><b>Two-phase init contract.</b> The runtime invokes {@link #setMetricGroup(MetricGroup)} after
 * {@link Plugin#configure(org.apache.flink.configuration.Configuration)} and before any operation
 * that would emit a metric. The call happens only from runtime entrypoints that own a process-level
 * {@link MetricGroup} (TaskManager and JobManager), via {@link
 * org.apache.flink.core.fs.FileSystem#attachMetrics(MetricGroup)}. Contexts without such a group
 * (CLI, HistoryServer, YARN client, embedded usage) never call it, in which case the plugin must
 * continue to operate normally and emit no metrics.
 *
 * <p><b>Idempotency.</b> {@code setMetricGroup} may be invoked more than once (for example if
 * metrics are re-attached). Implementations must treat repeated invocations idempotently: a call
 * with the same {@link MetricGroup} must not register duplicate metrics, and a call with a
 * different group should re-scope subsequently created metrics to the new group.
 *
 * <p>The {@link MetricGroup} is owned by the runtime; plugins must not close it. They may call
 * {@link MetricGroup#addGroup} on it freely to build nested label scopes.
 */
@PublicEvolving
public interface MetricsAware {

    /**
     * Hands the plugin a runtime-owned {@link MetricGroup} to register its metrics against. See the
     * class-level two-phase init contract.
     *
     * @param metricGroup the group to register metrics under; never {@code null}.
     */
    void setMetricGroup(MetricGroup metricGroup);
}
