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

package org.apache.flink.core.fs;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.plugin.MetricsAware;
import org.apache.flink.core.plugin.TestingPluginManager;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests for {@link FileSystem#attachMetrics(MetricGroup)} and the {@link MetricsAware} two-phase
 * init contract.
 *
 * <p>The headline case is {@link #attachMetricsReachesPluginLoadedMetricsAwareFactory()}: plugin
 * file systems are registered wrapped in a {@link PluginFileSystemFactory}, which does <em>not</em>
 * itself implement {@link MetricsAware}. {@code attachMetrics} must unwrap the proxy to reach the
 * real factory, otherwise the metric group is silently never delivered and no metrics are ever
 * emitted.
 */
class FileSystemAttachMetricsTest {

    @AfterEach
    void resetFileSystems() {
        // Restore the default, plugin-less factory registry so other tests are unaffected.
        FileSystem.initialize(new Configuration(), null);
    }

    @Test
    void attachMetricsReachesPluginLoadedMetricsAwareFactory() {
        RecordingMetricsAwareFactory factory = new RecordingMetricsAwareFactory("metrics-test-fs");
        initializeWithPlugins(factory);

        RecordingMetricGroup processGroup = new RecordingMetricGroup();
        FileSystem.attachMetrics(processGroup);

        // Unwrapped through PluginFileSystemFactory and invoked exactly once.
        assertThat(factory.setMetricGroupCalls).hasValue(1);
        // The group handed to the factory is the "filesystem" child of the process group, not the
        // process group itself.
        assertThat(processGroup.childGroupNames).containsExactly("filesystem");
        assertThat(factory.receivedGroup.get()).isNotNull().isNotSameAs(processGroup);
    }

    @Test
    void attachMetricsSkipsFactoriesThatAreNotMetricsAware() {
        initializeWithPlugins(new PlainFactory("plain-test-fs"));

        assertThatCode(() -> FileSystem.attachMetrics(new UnregisteredMetricsGroup()))
                .doesNotThrowAnyException();
    }

    @Test
    void attachMetricsIsResilientToAFactoryThatThrows() {
        RecordingMetricsAwareFactory ok = new RecordingMetricsAwareFactory("ok-test-fs");
        initializeWithPlugins(new ThrowingMetricsAwareFactory("throwing-test-fs"), ok);

        // A misbehaving plugin must never break process startup, and well-behaved factories must
        // still receive their group regardless of iteration order.
        assertThatCode(() -> FileSystem.attachMetrics(new UnregisteredMetricsGroup()))
                .doesNotThrowAnyException();
        assertThat(ok.setMetricGroupCalls).hasValue(1);
    }

    @Test
    void attachMetricsDoesNotThrowWhenInvokedRepeatedly() {
        RecordingMetricsAwareFactory factory = new RecordingMetricsAwareFactory("idem-test-fs");
        initializeWithPlugins(factory);

        MetricGroup group = new UnregisteredMetricsGroup();
        assertThatCode(
                        () -> {
                            FileSystem.attachMetrics(group);
                            FileSystem.attachMetrics(group);
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void setMetricGroupIsNotInvokedWhenAttachMetricsIsNeverCalled() {
        RecordingMetricsAwareFactory factory = new RecordingMetricsAwareFactory("never-test-fs");
        initializeWithPlugins(factory);

        assertThat(factory.setMetricGroupCalls).hasValue(0);
    }

    @Test
    void pluginFileSystemFactoryForwardsMetricGroupToInner() {
        RecordingMetricsAwareFactory inner = new RecordingMetricsAwareFactory("wrapped-fs");
        FileSystemFactory wrapper = PluginFileSystemFactory.of(inner);

        // The wrapper must itself be MetricsAware so attachMetrics reaches it without unwrapping.
        assertThat(wrapper).isInstanceOf(MetricsAware.class);

        MetricGroup group = new UnregisteredMetricsGroup();
        ((MetricsAware) wrapper).setMetricGroup(group);

        assertThat(inner.setMetricGroupCalls).hasValue(1);
        assertThat(inner.receivedGroup.get()).isSameAs(group);
    }

    private static void initializeWithPlugins(FileSystemFactory... factories) {
        Map<Class<?>, Iterator<?>> plugins = new HashMap<>();
        plugins.put(FileSystemFactory.class, Arrays.asList(factories).iterator());
        FileSystem.initialize(new Configuration(), new TestingPluginManager(plugins));
    }

    // ------------------------------------------------------------------------
    //  test factories
    // ------------------------------------------------------------------------

    private static class PlainFactory implements FileSystemFactory {
        private final String scheme;

        PlainFactory(String scheme) {
            this.scheme = scheme;
        }

        @Override
        public void configure(Configuration config) {}

        @Override
        public String getScheme() {
            return scheme;
        }

        @Override
        public FileSystem create(URI fsUri) throws IOException {
            throw new UnsupportedOperationException("not needed for this test");
        }
    }

    private static class RecordingMetricsAwareFactory extends PlainFactory implements MetricsAware {
        final AtomicInteger setMetricGroupCalls = new AtomicInteger();
        final AtomicReference<MetricGroup> receivedGroup = new AtomicReference<>();

        RecordingMetricsAwareFactory(String scheme) {
            super(scheme);
        }

        @Override
        public void setMetricGroup(MetricGroup metricGroup) {
            setMetricGroupCalls.incrementAndGet();
            receivedGroup.set(metricGroup);
        }
    }

    private static class ThrowingMetricsAwareFactory extends PlainFactory implements MetricsAware {
        ThrowingMetricsAwareFactory(String scheme) {
            super(scheme);
        }

        @Override
        public void setMetricGroup(MetricGroup metricGroup) {
            throw new RuntimeException("intentional failure from a misbehaving plugin");
        }
    }

    /**
     * Records the names of child groups created directly under it, sharing the list with children.
     */
    private static class RecordingMetricGroup extends UnregisteredMetricsGroup {
        final List<String> childGroupNames;

        RecordingMetricGroup() {
            this(Collections.synchronizedList(new ArrayList<>()));
        }

        private RecordingMetricGroup(List<String> childGroupNames) {
            this.childGroupNames = childGroupNames;
        }

        @Override
        public MetricGroup addGroup(String name) {
            childGroupNames.add(name);
            return new RecordingMetricGroup(childGroupNames);
        }
    }
}
