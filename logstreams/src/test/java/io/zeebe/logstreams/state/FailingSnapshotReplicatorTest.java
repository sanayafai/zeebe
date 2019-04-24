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
package io.zeebe.logstreams.state;

import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.logstreams.processor.SnapshotChunk;
import io.zeebe.logstreams.processor.SnapshotReplication;
import io.zeebe.logstreams.util.RocksDBWrapper;
import io.zeebe.test.util.AutoCloseableRule;
import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FailingSnapshotReplicatorTest {
  @Rule public TemporaryFolder tempFolderRule = new TemporaryFolder();
  @Rule public AutoCloseableRule autoCloseableRule = new AutoCloseableRule();

  private StateSnapshotController replicatorSnapshotController;
  private FailingReplicator replicator;

  @Before
  public void setup() throws IOException {
    final File runtimeDirectory = tempFolderRule.newFolder("runtime");
    final File snapshotsDirectory = tempFolderRule.newFolder("snapshots");
    final StateStorage storage = new StateStorage(runtimeDirectory, snapshotsDirectory);

    replicator = new FailingReplicator();
    replicatorSnapshotController =
        new StateSnapshotController(
            ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class), storage, replicator);

    autoCloseableRule.manage(replicatorSnapshotController);

    final String key = "test";
    final int value = 0xCAFE;
    final RocksDBWrapper wrapper = new RocksDBWrapper();
    wrapper.wrap(replicatorSnapshotController.openDb());
    wrapper.putInt(key, value);
  }

  @Test(expected = RuntimeException.class)
  public void shouldFailToExecuteReplication() {
    // given
    replicatorSnapshotController.takeSnapshot(1);

    // when
    replicatorSnapshotController.replicateLatestSnapshot(
        replication -> {
          throw new RuntimeException("excepted");
        });

    // then
  }

  @Test(expected = RuntimeException.class)
  public void shouldFailToReplicateSnapshotChunks() {
    // given
    replicatorSnapshotController.takeSnapshot(1);

    // when
    replicatorSnapshotController.replicateLatestSnapshot(Runnable::run);

    // then
  }

  private final class FailingReplicator implements SnapshotReplication {

    @Override
    public void replicate(SnapshotChunk snapshot) {
      throw new RuntimeException("expected");
    }

    @Override
    public void consume(Consumer<SnapshotChunk> consumer) {}

    @Override
    public void close() {}
  }
}
