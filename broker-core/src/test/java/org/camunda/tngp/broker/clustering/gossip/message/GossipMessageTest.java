package org.camunda.tngp.broker.clustering.gossip.message;

import static org.assertj.core.api.Assertions.*;
import static org.camunda.tngp.test.util.BufferWriterUtil.*;
import static org.camunda.tngp.util.buffer.BufferUtil.*;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.camunda.tngp.broker.clustering.gossip.data.Peer;
import org.camunda.tngp.broker.clustering.gossip.data.PeerList;
import org.camunda.tngp.broker.clustering.gossip.data.RaftMembership;
import org.camunda.tngp.clustering.gossip.PeerState;
import org.camunda.tngp.clustering.gossip.RaftMembershipState;
import org.camunda.tngp.test.util.BufferWriterUtil;
import org.camunda.tngp.transport.SocketAddress;
import org.junit.Test;


public class GossipMessageTest
{

    @Test
    public void testGossipRequest()
    {
        final PeerList expected = new PeerList(2);
        expected.append(
            new Peer()
                .alive()
        );
        expected.append(
            new Peer()
                .dead()
        );

        final GossipRequest gossipRequest = new GossipRequest()
            .peers(expected);

        final Iterator<Peer> actual = writeAndRead(gossipRequest).peers();

        assertThat(actual)
            .usingElementComparatorOnFields(
                "clientEndpoint",
                "managementEndpoint",
                "replicationEndpoint",
                "heartbeat",
                "state"
            )
            .hasSameElementsAs(expected);
    }

    @Test
    public void testGossipResponse()
    {
        final PeerList expected = new PeerList(2);
        expected.append(
            new Peer()
                .alive()
        );
        expected.append(
            new Peer()
                .dead()
        );

        final GossipResponse gossipResponse = new GossipResponse()
            .peers(expected);

        final Iterator<Peer> actual = writeAndRead(gossipResponse).peers();

        assertThat(actual)
            .usingElementComparatorOnFields(
                "clientEndpoint",
                "managementEndpoint",
                "replicationEndpoint",
                "heartbeat",
                "state"
            )
            .hasSameElementsAs(expected);
    }

    @Test
    public void testProbeRequest()
    {
        final ProbeRequest probeRequest = new ProbeRequest()
            .target(
                new SocketAddress()
                    .host("test")
                    .port(111)
            );

        assertEqualFieldsAfterWriteAndRead(probeRequest,
            "target"
        );
    }

    @Test
    public void testPeer()
    {
        final Peer peer = new Peer()
            .state(PeerState.SUSPECT)
            .changeStateTime(444);

        peer.heartbeat()
            .generation(1234)
            .version(5678);

        peer.clientEndpoint()
            .host("client")
            .port(111);

        peer.managementEndpoint()
            .host("management")
            .port(222);

        peer.replicationEndpoint()
            .host("replication")
            .port(333);

        final DirectBuffer firstTopicName = wrapString("first");
        final DirectBuffer secondTopicName = wrapString("second");

        peer.raftMemberships()
            .add(
                new RaftMembership()
                    .topicName(firstTopicName, 0, firstTopicName.capacity())
                    .partitionId(555)
                    .term(666)
                    .state(RaftMembershipState.CANDIDATE)
            )
            .add(
                new RaftMembership()
                    .topicName(secondTopicName, 0, secondTopicName.capacity())
                    .partitionId(777)
                    .term(888)
                    .state(RaftMembershipState.LEADER)
            );


        final Peer actual = BufferWriterUtil.writeAndRead(peer);
        assertThat(actual)
            .isEqualToComparingOnlyGivenFields(peer,
                "clientEndpoint",
                "managementEndpoint",
                "replicationEndpoint",
                "heartbeat",
                "state",
                "changeStateTime"
            );

        assertThat(actual.raftMemberships())
            .hasSameElementsAs(peer.raftMemberships());
    }

}
