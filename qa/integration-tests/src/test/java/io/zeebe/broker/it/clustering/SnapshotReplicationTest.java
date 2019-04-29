/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.clustering;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.Loggers;
import io.zeebe.broker.it.DataDeleteTest;
import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.client.ZeebeClient;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.MessageIntent;
import io.zeebe.test.util.TestUtil;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class SnapshotReplicationTest {

  private static final int PARTITION_COUNT = 1;

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process").startEvent().endEvent().done();

  // NOTE: the configuration removes the RecordingExporter from the broker's configuration to enable
  // data deletion so it can't be used in tests
  public ClusteringRule clusteringRule =
      new ClusteringRule(PARTITION_COUNT, 3, 3, DataDeleteTest::configureForDeletionTest);
  public GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(clusteringRule).around(clientRule);

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private ZeebeClient client;

  @Before
  public void init() {
    client = clientRule.getClient();
    clusteringRule.getClock().pinCurrentTime();
  }

  @Test
  public void shouldReplicateSnapshots() throws Exception {
    // given
    client.newDeployCommand().addWorkflowModel(WORKFLOW, "workflow.bpmn").send().join();
    final int leaderNodeId = clusteringRule.getLeaderForPartition(1).getNodeId();
    final Broker leader = clusteringRule.getBroker(leaderNodeId);
    clusteringRule.getClock().addTime(Duration.ofSeconds(DataDeleteTest.SNAPSHOT_PERIOD_SECONDS));

    // when - snapshot
    waitForValidSnapshotAtBroker(leader);

    final List<Broker> otherBrokers = clusteringRule.getOtherBrokerObjects(leaderNodeId);
    for (Broker broker : otherBrokers) {
      waitForValidSnapshotAtBroker(broker);
    }

    // then - replicated
    final Collection<Broker> brokers = clusteringRule.getBrokers();
    final Map<Integer, Map<String, Long>> brokerSnapshotChecksums = new HashMap<>();
    for (Broker broker : brokers) {
      final Map<String, Long> checksums = createSnapshotDirectoryChecksums(broker);
      brokerSnapshotChecksums.put(broker.getConfig().getCluster().getNodeId(), checksums);
    }

    final Map<String, Long> checksumFirstNode = brokerSnapshotChecksums.get(0);
    assertThat(checksumFirstNode).isEqualTo(brokerSnapshotChecksums.get(1));
    assertThat(checksumFirstNode).isEqualTo(brokerSnapshotChecksums.get(2));
  }

  @Test
  public void shouldDeleteDataOnFollowersWithExporter() {
    // given
    final int leaderNodeId = clusteringRule.getLeaderForPartition(1).getNodeId();
    final List<Broker> followers =
        clusteringRule.getBrokers().stream()
            .filter(b -> b.getConfig().getCluster().getNodeId() != leaderNodeId)
            .collect(Collectors.toList());

    // when
    final AtomicInteger messagesSent = new AtomicInteger();
    while (followers.stream()
        .map(this::getSegmentsDirectory)
        .allMatch(dir -> dir.listFiles().length <= 2)) {
      clientRule
          .getClient()
          .newPublishMessageCommand()
          .messageName("msg")
          .correlationKey("key")
          .send()
          .join();
      messagesSent.incrementAndGet();
    }

    // when
    TestUtil.waitUntil(
        () ->
            DataDeleteTest.TestExporter.records.stream()
                    .filter(r -> r.getMetadata().getIntent() == MessageIntent.PUBLISHED)
                    .limit(messagesSent.get())
                    .count()
                == messagesSent.get());

    // then
    takeSnapshotAndAssertDataWasDeleted(followers);
  }

  private void takeSnapshotAndAssertDataWasDeleted(final List<Broker> followers) {
    final HashMap<Integer, Integer> followerSegmentCounts = new HashMap();
    followers.forEach(
        b -> {
          final int nodeId = b.getConfig().getCluster().getNodeId();
          followerSegmentCounts.put(nodeId, getSegmentsDirectory(b).list().length);
        });

    clusteringRule.getClock().addTime(Duration.ofSeconds(DataDeleteTest.SNAPSHOT_PERIOD_SECONDS));
    followers.forEach(this::waitForValidSnapshotAtBroker);

    // then
    TestUtil.waitUntil(
        () ->
            followers.stream()
                .allMatch(
                    b ->
                        getSegmentsDirectory(b).listFiles().length
                            < followerSegmentCounts.get(b.getConfig().getCluster().getNodeId())));
  }

  private File getSnapshotsDirectory(Broker broker) {
    final String dataDir = broker.getConfig().getData().getDirectories().get(0);
    return new File(dataDir, "partition-1/state/1_zb-stream-processor/snapshots");
  }

  private File getSegmentsDirectory(Broker broker) {
    final String dataDir = broker.getConfig().getData().getDirectories().get(0);
    return new File(dataDir, "/partition-1/segments");
  }

  private void waitForValidSnapshotAtBroker(Broker broker) {
    final File snapshotsDir = getSnapshotsDirectory(broker);

    waitUntil(
        () -> Arrays.stream(snapshotsDir.listFiles()).anyMatch(f -> !f.getName().contains("tmp")));
  }

  private Map<String, Long> createSnapshotDirectoryChecksums(Broker broker) throws Exception {
    final File snapshotsDir = getSnapshotsDirectory(broker);

    final Map<String, Long> checksums = createChecksumsForSnapshotDirectory(snapshotsDir);

    assertThat(checksums.size()).isGreaterThan(0);
    return checksums;
  }

  private Map<String, Long> createChecksumsForSnapshotDirectory(File snapshotDirectory) {
    final Map<String, Long> checksums = new HashMap<>();
    final File[] snapshotDirs = snapshotDirectory.listFiles();
    if (snapshotDirs != null) {
      Arrays.stream(snapshotDirs)
          .filter(f -> !f.getName().contains("tmp"))
          .forEach(
              validSnapshotDir -> {
                final File[] snapshotFiles = validSnapshotDir.listFiles();
                if (snapshotFiles != null) {
                  for (File snapshotFile : snapshotFiles) {
                    final long checksum = createCheckSumForFile(snapshotFile);

                    Loggers.STREAM_PROCESSING.debug(
                        "Created checksum {} for file {}", checksum, snapshotFile);
                    checksums.put(snapshotFile.getName(), checksum);
                  }
                }
              });
    }

    return checksums;
  }

  private long createCheckSumForFile(File snapshotFile) {
    try (CheckedInputStream checkedInputStream =
        new CheckedInputStream(Files.newInputStream(snapshotFile.toPath()), new CRC32())) {
      while (checkedInputStream.skip(512) > 0) {}

      return checkedInputStream.getChecksum().getValue();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
