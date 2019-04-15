/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.logstreams.state;

import static io.zeebe.broker.Loggers.STATE_LOGGER;

import io.atomix.cluster.messaging.ClusterEventService;
import io.zeebe.logstreams.processor.SnapshotChunk;
import io.zeebe.logstreams.processor.SnapshotReplication;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

public class StateReplication implements SnapshotReplication {

  public static final String REPLICATION_TOPIC_FORMAT = "replication-%d-%s";
  private static final Logger LOG = STATE_LOGGER;

  private final String replicationTopic;

  private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();
  private final DirectBuffer readBuffer = new UnsafeBuffer(0, 0);
  private final ClusterEventService eventService;
  private ExecutorService executorService;

  public StateReplication(ClusterEventService eventService, int partitionId, String name) {
    this.eventService = eventService;
    this.replicationTopic = String.format(REPLICATION_TOPIC_FORMAT, partitionId, name);
  }

  @Override
  public void replicate(SnapshotChunk snapshot) {
    eventService.broadcast(
        replicationTopic,
        snapshot,
        (s) -> {
          LOG.debug(
              "Replicate on topic {} snapshot chunk {} for snapshot pos {}.",
              replicationTopic,
              s.getChunkName(),
              s.getSnapshotPosition());
          int offset = 0;
          writeBuffer.putLong(offset, s.getSnapshotPosition());
          offset += Long.BYTES;

          writeBuffer.putInt(offset, s.getTotalCount());
          offset += Integer.BYTES;

          final String chunkName = s.getChunkName();
          final int chunkNameLength = chunkName.length();
          writeBuffer.putInt(offset, chunkNameLength);
          offset += Integer.BYTES;

          writeBuffer.putBytes(offset, chunkName.getBytes());
          offset += chunkNameLength;

          final byte[] content = s.getContent();
          final int contentLength = content.length;
          writeBuffer.putInt(offset, contentLength);
          offset += Integer.BYTES;

          writeBuffer.putBytes(offset, s.getContent());
          offset += contentLength;

          final byte[] message = new byte[offset];
          writeBuffer.getBytes(0, message);
          return message;
        });
  }

  @Override
  public void consume(Consumer<SnapshotChunk> consumer) {
    executorService = Executors.newSingleThreadExecutor();
    eventService.subscribe(
        replicationTopic,
        (bytes -> {
          readBuffer.wrap(bytes);
          final SnapshotChunkView snapshotChunkView = new SnapshotChunkView(readBuffer);
          LOG.debug(
              "Received on topic {} replicated snapshot chunk {} for snapshot pos {}.",
              replicationTopic,
              snapshotChunkView.getChunkName(),
              snapshotChunkView.getSnapshotPosition());
          return snapshotChunkView;
        }),
        consumer,
        executorService);
  }

  @Override
  public void close() {
    if (executorService != null) {
      executorService.shutdownNow();
      executorService = null;
    }
  }

  private static final class SnapshotChunkView implements SnapshotChunk {
    private final DirectBuffer view;

    SnapshotChunkView(DirectBuffer view) {
      this.view = view;
    }

    @Override
    public long getSnapshotPosition() {
      final int offset = 0;
      return view.getLong(offset);
    }

    @Override
    public int getTotalCount() {
      final int offset = Long.BYTES;
      return view.getInt(offset);
    }

    @Override
    public String getChunkName() {
      int offset = Long.BYTES + Integer.BYTES;
      final int chunkLength = view.getInt(offset);
      offset += Integer.BYTES;

      return view.getStringWithoutLengthAscii(offset, chunkLength);
    }

    @Override
    public byte[] getContent() {
      int offset = Long.BYTES + Integer.BYTES;
      final int chunkLength = view.getInt(offset);
      offset += Integer.BYTES + chunkLength;

      final int contentLength = view.getInt(offset);
      offset += Integer.BYTES;

      final byte[] content = new byte[contentLength];
      view.getBytes(offset, content);
      return content;
    }
  }
}
