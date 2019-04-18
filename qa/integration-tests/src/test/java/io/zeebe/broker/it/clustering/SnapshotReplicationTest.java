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

import static io.zeebe.broker.logstreams.state.StateReplication.REPLICATION_TOPIC_FORMAT;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.cluster.AtomixCluster;
import io.zeebe.broker.Broker;
import io.zeebe.broker.exporter.ExporterManagerService;
import io.zeebe.broker.exporter.stream.ExporterColumnFamilies;
import io.zeebe.broker.it.GrpcClientRule;
import io.zeebe.broker.logstreams.ZbStreamProcessorService;
import io.zeebe.broker.logstreams.state.DefaultZeebeDbFactory;
import io.zeebe.broker.logstreams.state.StateStorageFactory;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.commands.BrokerInfo;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import java.io.File;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class SnapshotReplicationTest {

  private static final int PARTITION_COUNT = 1;

  private static final BpmnModelInstance WORKFLOW =
      Bpmn.createExecutableProcess("process").startEvent().endEvent().done();

  public ClusteringRule clusteringRule =
      new ClusteringRule(
          PARTITION_COUNT,
          3,
          3,
          brokerCfg -> {
            brokerCfg.getData().setSnapshotPeriod("2s");
          });
  public GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(clusteringRule).around(clientRule);

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private ZeebeClient client;

  @Before
  public void init() {
    client = clientRule.getClient();
  }

  @Test
  public void shouldReplicateSnapshots() throws Exception {
    // given
    client.newDeployCommand().addWorkflowModel(WORKFLOW, "workflow.bpmn").send().join();
    final CountDownLatch snapshotLatch = new CountDownLatch(2);
    subscribeToReplication(snapshotLatch);

    // when - snapshot
    snapshotLatch.await(5_000L, TimeUnit.MILLISECONDS);

    // then - replicated
    final BrokerInfo leaderForPartition = clusteringRule.getLeaderForPartition(1);

    final Collection<Broker> brokers = clusteringRule.getBrokers();
    for (Broker broker : brokers) {
      final boolean isNoLeader =
          broker.getConfig().getCluster().getNodeId() != leaderForPartition.getNodeId();
      if (isNoLeader) {
        final String dataRoot = broker.getConfig().getData().getDirectories().get(0);
        validateSnapshots(dataRoot);
      }
    }
  }

  private void subscribeToReplication(CountDownLatch snapshotLatch) {
    final AtomixCluster atomixCluster = clusteringRule.getAtomixCluster();
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    final String exporterTopicReplication =
        String.format(REPLICATION_TOPIC_FORMAT, 1, ExporterManagerService.PROCESSOR_NAME);
    atomixCluster
        .getEventService()
        .subscribe(
            exporterTopicReplication,
            bytes -> {
              snapshotLatch.countDown();
              return bytes;
            },
            bytes -> {},
            executorService);

    final String processorTopicReplication =
        String.format(REPLICATION_TOPIC_FORMAT, 1, ZbStreamProcessorService.PROCESSOR_NAME);
    atomixCluster
        .getEventService()
        .subscribe(
            processorTopicReplication,
            bytes -> {
              snapshotLatch.countDown();
              return bytes;
            },
            bytes -> {},
            executorService);
  }

  private void validateSnapshots(String dataRoot) throws Exception {
    final StateStorageFactory factory =
        new StateStorageFactory(new File(dataRoot, "partition-1/state"));
    final StateStorage exporterStateStorage =
        factory.create(
            ExporterManagerService.EXPORTER_PROCESSOR_ID, ExporterManagerService.PROCESSOR_NAME);

    waitUntil(() -> exporterStateStorage.listByPositionAsc().size() > 0);

    final StateSnapshotController exportSnapshotController =
        new StateSnapshotController(
            DefaultZeebeDbFactory.defaultFactory(ExporterColumnFamilies.class),
            exporterStateStorage);

    final StateStorage processorStateStorage =
        factory.create(1, ZbStreamProcessorService.PROCESSOR_NAME);

    waitUntil(() -> processorStateStorage.listByPositionAsc().size() > 0);

    final StateSnapshotController processorSnapshotController =
        new StateSnapshotController(
            DefaultZeebeDbFactory.DEFAULT_DB_FACTORY, processorStateStorage);

    // expect no exception
    assertThat(exportSnapshotController.recover()).isGreaterThan(0);
    assertThat(processorSnapshotController.recover()).isGreaterThan(0);
  }
}
