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

package org.apache.flink.fs.s3native.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.fs.s3native.NativeS3FileSystemFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.util.AutoCloseableAsync;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.FileNotFoundException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test proving that real S3 operations performed through {@code NativeS3FileSystem} are
 * translated into Flink metrics by {@link AwsSdkMetricBridge} and become visible in a real Flink
 * metric registry.
 *
 * <p>Unlike {@link AwsSdkMetricBridgeTest} (which drives the bridge with synthesized SDK records
 * and a fake {@code MetricGroup}), this test exercises the full chain: the AWS SDK actually invokes
 * the registered {@link software.amazon.awssdk.metrics.MetricPublisher} after each completed API
 * call, the bridge registers and updates {@link Counter}/{@link Histogram} handles, and the
 * assertions read those handles back through {@link MetricListener}'s real {@code MetricRegistry}.
 *
 * <p>Assertions use only GET/HEAD/LIST round trips, which carry no request body and are therefore
 * unaffected by the request-checksum behaviour newer AWS SDK versions apply to {@code PutObject}.
 *
 * <p>Requires Docker; auto-skipped when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class NativeS3MetricsEmissionITCase {

    private static final String MINIO_IMAGE = "minio/minio:RELEASE.2022-02-07T08-17-33Z";
    private static final int MINIO_PORT = 9000;
    private static final String ACCESS_KEY = "metricsAccessKey";
    private static final String SECRET_KEY = "metricsSecretKey";
    private static final String BUCKET = "flip576-metrics";

    @Container
    private static final GenericContainer<?> MINIO =
            new GenericContainer<>(MINIO_IMAGE)
                    .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                    .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
                    .withCommand("server", "/data")
                    .withExposedPorts(MINIO_PORT)
                    .waitingFor(
                            Wait.forHttp("/minio/health/ready")
                                    .forPort(MINIO_PORT)
                                    .withStartupTimeout(Duration.ofMinutes(2)));

    private static String endpoint() {
        return String.format("http://%s:%d", MINIO.getHost(), MINIO.getMappedPort(MINIO_PORT));
    }

    @BeforeAll
    static void createBucket() {
        try (S3Client client =
                S3Client.builder()
                        .endpointOverride(URI.create(endpoint()))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                                StaticCredentialsProvider.create(
                                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                        .serviceConfiguration(
                                S3Configuration.builder().pathStyleAccessEnabled(true).build())
                        .build()) {
            client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }

    @Test
    void realS3OperationsEmitFlinkMetrics() throws Exception {
        Configuration config = new Configuration();
        config.set(NativeS3FileSystemFactory.ENDPOINT, endpoint());
        config.set(NativeS3FileSystemFactory.ACCESS_KEY, ACCESS_KEY);
        config.set(NativeS3FileSystemFactory.SECRET_KEY, SECRET_KEY);
        config.set(NativeS3FileSystemFactory.REGION, "us-east-1");
        config.set(NativeS3FileSystemFactory.PATH_STYLE_ACCESS, true);
        config.set(NativeS3FileSystemFactory.CHUNKED_ENCODING_ENABLED, false);
        config.set(NativeS3FileSystemFactory.CHECKSUM_VALIDATION_ENABLED, false);
        config.set(NativeS3FileSystemFactory.METRICS_ENABLED, true);

        NativeS3FileSystemFactory factory = new NativeS3FileSystemFactory();
        factory.configure(config);

        MetricListener metricListener = new MetricListener();
        // Mirror what FileSystem#attachMetrics hands to the factory: the "filesystem" child of the
        // process-level group.
        MetricGroup fsGroup = metricListener.getMetricGroup().addGroup("filesystem");
        factory.setMetricGroup(fsGroup);

        FileSystem fs = factory.create(URI.create("s3://" + BUCKET + "/"));
        try {
            // (1) A successful listing -> ListObjectsV2 (2xx). No request body, no checksum.
            FileStatus[] listing = fs.listStatus(new Path("s3://" + BUCKET + "/"));
            assertThat(listing).isNotNull();

            // (2) A lookup of a key that does not exist -> HeadObject classified as an error
            // (4xx).
            try {
                fs.getFileStatus(
                        new Path("s3://" + BUCKET + "/does-not-exist-" + System.nanoTime()));
            } catch (FileNotFoundException expected) {
                // expected: the object is absent
            }

            // The SDK publishes metrics after each completed call; the sync client typically
            // publishes inline, but poll to remain robust against any asynchronous delivery.
            CommonTestUtils.waitUtil(
                    () -> listObjectsSuccessCount(metricListener) > 0L,
                    Duration.ofSeconds(30),
                    "Expected a ListObjectsV2 api_call_count metric to be emitted by real S3 traffic");

            // --- api_call_count (Counter) for the successful listing ---
            long listCalls = listObjectsSuccessCount(metricListener);
            assertThat(listCalls).as("ListObjectsV2 (2xx) api_call_count").isGreaterThan(0L);

            // --- api_call_duration_ms (Histogram) for the listing ---
            Optional<Histogram> listDuration =
                    metricListener.getHistogram(
                            "filesystem",
                            "filesystem_type",
                            "s3",
                            "op",
                            "ListObjectsV2",
                            "api_call_duration_ms");
            assertThat(listDuration).as("ListObjectsV2 duration histogram").isPresent();
            assertThat(listDuration.get().getCount()).isGreaterThan(0L);

            // --- the failed lookup is recorded and classified as a client error (4xx) ---
            long headErrorCalls =
                    counter(
                            metricListener,
                            "filesystem",
                            "filesystem_type",
                            "s3",
                            "op",
                            "HeadObject",
                            "status_class",
                            "4xx",
                            "api_call_count");
            assertThat(headErrorCalls)
                    .as("HeadObject (4xx) api_call_count for the missing key")
                    .isGreaterThan(0L);
        } finally {
            ((AutoCloseableAsync) fs).closeAsync().get();
        }
    }

    private static long listObjectsSuccessCount(MetricListener listener) {
        return counter(
                listener,
                "filesystem",
                "filesystem_type",
                "s3",
                "op",
                "ListObjectsV2",
                "status_class",
                "2xx",
                "api_call_count");
    }

    private static long counter(MetricListener listener, String... identifier) {
        return listener.getCounter(identifier).map(Counter::getCount).orElse(0L);
    }
}
