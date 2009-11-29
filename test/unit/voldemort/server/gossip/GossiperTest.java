package voldemort.server.gossip;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.ProtoBuffAdminClientRequestFormat;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author afeinberg
 */
public class GossiperTest extends TestCase {
    private List<VoldemortServer> servers = new ArrayList<VoldemortServer>();
    private Cluster cluster;

    private static String testStoreName = "test-replication-memory";
    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    @Override
    public void setUp() throws IOException {
        cluster = ServerTestUtils.getLocalCluster(2, new int[][] { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } });
        servers.add(ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(0,
                TestUtils.createTempDir()
                        .getAbsolutePath(),
                null,
                storesXmlfile),
                cluster));
        servers.add(ServerTestUtils.startVoldemortServer(ServerTestUtils.createServerConfig(1,
                TestUtils.createTempDir()
                        .getAbsolutePath(),
                null,
                storesXmlfile),
                cluster));
    }

    private AdminClient getAdminClient(Cluster newCluster, VoldemortConfig newServerConfig) {
        ClientConfig clientConfig = new ClientConfig()
                .setMaxConnectionsPerNode(8)
                .setMaxThreads(8)
                .setConnectionTimeout(newServerConfig.getAdminConnectionTimeout(), TimeUnit.MILLISECONDS)
                .setSocketTimeout(newServerConfig.getSocketTimeoutMs(), TimeUnit.MILLISECONDS)
                .setSocketBufferSize(newServerConfig.getAdminSocketBufferSize());
        return new ProtoBuffAdminClientRequestFormat(newCluster, clientConfig);
    }

    public void testGossiper() throws IOException {
        // First create a new cluster:
        // Allocate ports for all nodes in the new cluster, to match existing cluster
        int portIdx = 0;
        int ports[] = new int[3 * (cluster.getNumberOfNodes()+1)];
        for (Node node: cluster.getNodes()) {
            ports[portIdx++] = node.getHttpPort();
            ports[portIdx++] = node.getSocketPort();
            ports[portIdx++] = node.getAdminPort();
        }
        
        ports[portIdx++] = ServerTestUtils.findFreePort();
        ports[portIdx++] = ServerTestUtils.findFreePort();
        ports[portIdx] = ServerTestUtils.findFreePort();

        // Create a new partitioning scheme with room for a new server
        Cluster newCluster = ServerTestUtils.getLocalCluster(cluster.getNumberOfNodes() + 1,
                ports, new int[][] { {0, 1, 2}, {3, 4, 5}, {6, 7} });

        // Start the new server
        VoldemortServer newServer = ServerTestUtils.startVoldemortServer(ServerTestUtils
                .createServerConfig(2, TestUtils.createTempDir()
                .getAbsolutePath(),
                null,
                storesXmlfile),
                newCluster);
        servers.add(newServer);

       // Wait a while until it starts
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Get the new cluster.XML
        AdminClient localAdminClient = getAdminClient(newCluster, newServer.getVoldemortConfig());

        Versioned<String> versionedClusterXML = localAdminClient.getRemoteMetadata(2, MetadataStore.CLUSTER_KEY);

        // Increment the version, let what would be the "donor node" know about it
        // (this will seed the gossip)
        Version version = versionedClusterXML.getVersion();
        ((VectorClock) version).incrementVersion(2, ((VectorClock) version).getTimestamp() + 1);
        ((VectorClock) version).incrementVersion(0, ((VectorClock) version).getTimestamp() + 1);

        localAdminClient.updateRemoteMetadata(0, MetadataStore.CLUSTER_KEY, versionedClusterXML);
        localAdminClient.updateRemoteMetadata(2, MetadataStore.CLUSTER_KEY, versionedClusterXML);

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Start a thread pool for gossipers and start gossiping
        ExecutorService executorService = Executors.newFixedThreadPool(newCluster.getNumberOfNodes() + 1);

        List<Gossiper> gossipers = new ArrayList<Gossiper>(newCluster.getNumberOfNodes());
        for (VoldemortServer server: servers) {
            Gossiper gossiper = new Gossiper(server.getMetadataStore(),
                getAdminClient(server.getMetadataStore().getCluster(), server.getVoldemortConfig()), 50);
            gossiper.start();
            executorService.submit(gossiper);
            gossipers.add(gossiper);
        }

        int serversSeen = 0;
        // Wait a second for gossip to spread
        try {
            Thread.sleep(1000);

            // Now verify that we have gossiped correctly
            for (VoldemortServer server: servers) {
                Cluster clusterAtServer = server.getMetadataStore().getCluster();
                int nodeId = server.getMetadataStore().getNodeId();
                assertEquals("server " + nodeId + " has heard " +
                        " the gossip about number of nodes", clusterAtServer.getNumberOfNodes(),
                        newCluster.getNumberOfNodes());
                assertEquals("server " + nodeId + " has heard " + " the gossip about partitions",
                        clusterAtServer.getNodeById(nodeId).getPartitionIds(),
                        newCluster.getNodeById(nodeId).getPartitionIds());
                serversSeen++;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            for (Gossiper gossiper: gossipers)
                gossiper.stop();
        }
        assertEquals("saw all servers", serversSeen, servers.size());
    }
}
