package voldemort.hashtrees;

import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_HTREE_SERVER_PORT_NO;
import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_SEG_DATA_BLOCKS_COUNT;
import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_TREE_ID;
import static voldemort.hashtrees.HashTreeImplTestUtils.ROOT_NODE;
import static voldemort.hashtrees.HashTreeImplTestUtils.createHashTree;
import static voldemort.hashtrees.HashTreeImplTestUtils.generateInMemoryAndPersistentStores;
import static voldemort.hashtrees.HashTreeImplTestUtils.generateInMemoryStore;
import static voldemort.hashtrees.HashTreeImplTestUtils.randomByteBuffer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import voldemort.hashtrees.HashTreeImplTestUtils.HTreeComponents;
import voldemort.hashtrees.HashTreeImplTestUtils.StorageImplTest;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.Storage;
import voldemort.hashtrees.tasks.BGTasksManager;
import voldemort.hashtrees.thrift.generated.SegmentHash;

public class HashTreeImplIntegerationTest {

    private static void waitForTheEvent(BlockingQueue<HashTreeImplEvent> events,
                                        HashTreeImplEvent expectedEvent,
                                        long maxWaitTime) throws InterruptedException {
        HashTreeImplEvent event = null;
        long cntr = System.currentTimeMillis();
        while(true) {
            event = events.poll(1000, TimeUnit.MILLISECONDS);
            if(event == expectedEvent)
                break;
            else if(event == null) {
                long diff = System.currentTimeMillis() - cntr;
                if(diff > maxWaitTime)
                    break;
            }
        }
        Assert.assertNotNull(event);
        Assert.assertEquals(event, expectedEvent);
    }

    @Test
    public void testSegmentUpdate() throws TTransportException, InterruptedException {
        HashTreeStorage htStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        BlockingQueue<HashTreeImplEvent> events = new ArrayBlockingQueue<HashTreeImplEvent>(10);
        Storage storage = new StorageImplTest();
        HashTreeImplTestObj hTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                            htStorage,
                                                            storage,
                                                            events);
        BGTasksManager bgTasks = new BGTasksManager(hTree,
                                                    Executors.newFixedThreadPool(10),
                                                    DEFAULT_HTREE_SERVER_PORT_NO,
                                                    1000,
                                                    300000,
                                                    1000,
                                                    30000);
        hTree.startBGTasks(bgTasks);
        waitForTheEvent(events, HashTreeImplEvent.UPDATE_SEGMENT, 10000);
        hTree.shutdown();
    }

    @Test
    public void testFullTreeUpdate() throws TTransportException, InterruptedException {
        HashTreeStorage htStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        BlockingQueue<HashTreeImplEvent> events = new ArrayBlockingQueue<HashTreeImplEvent>(10);
        Storage storage = new StorageImplTest();
        HashTreeImplTestObj hTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                            htStorage,
                                                            storage,
                                                            events);
        BGTasksManager bgTasks = new BGTasksManager(hTree,
                                                    Executors.newFixedThreadPool(10),
                                                    DEFAULT_HTREE_SERVER_PORT_NO,
                                                    1000,
                                                    1000,
                                                    30000,
                                                    30000);
        hTree.startBGTasks(bgTasks);
        waitForTheEvent(events, HashTreeImplEvent.UPDATE_FULL_TREE, 10000);
        hTree.shutdown();
    }

    @Test
    public void testSynch() throws IOException, TException {
        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage[] remoteStores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        for(int j = 0; j <= 1; j++) {
            HTreeComponents localHTreeComp = createHashTree(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                            stores[j]);
            HTreeComponents remoteHTreeComp = createHashTree(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                             remoteStores[j]);

            for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                localHTreeComp.storage.put(randomByteBuffer(), randomByteBuffer());
            }

            localHTreeComp.hTree.updateHashTrees(false);
            boolean anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
            Assert.assertTrue(anyUpdates);

            remoteHTreeComp.hTree.updateHashTrees(false);
            anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
            Assert.assertFalse(anyUpdates);

            SegmentHash localRootHash = localHTreeComp.hTree.getSegmentHash(DEFAULT_TREE_ID,
                                                                            ROOT_NODE);
            Assert.assertNotNull(localRootHash);
            SegmentHash remoteRootHash = remoteHTreeComp.hTree.getSegmentHash(DEFAULT_TREE_ID,
                                                                              ROOT_NODE);
            Assert.assertNotNull(remoteRootHash);

            Assert.assertTrue(Arrays.equals(localRootHash.getHash(), remoteRootHash.getHash()));
        }
    }

}
