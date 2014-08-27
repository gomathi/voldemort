package voldemort.hashtrees;

import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_SEG_DATA_BLOCKS_COUNT;
import static voldemort.hashtrees.HashTreeImplTestUtils.generateInMemoryStore;
import static voldemort.hashtrees.HashTreeImplTestUtils.treeIdProvider;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.hashtrees.HashTreeImplTestUtils.StorageImplTest;
import voldemort.hashtrees.core.HashTreeConstants;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.Storage;
import voldemort.hashtrees.synch.HashTreeSyncManagerImpl;

public class HashTreeImplIntegerationTest {

    private static void waitForTheEvent(BlockingQueue<HashTreeImplTestEvent> events,
                                        HashTreeImplTestEvent expectedEvent,
                                        long maxWaitTime) throws InterruptedException {
        HashTreeImplTestEvent event = null;
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
    public void testSegmentUpdate() throws InterruptedException {
        HashTreeStorage htStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        htStorage.setLastFullyTreeBuiltTimestamp(1, System.currentTimeMillis());
        BlockingQueue<HashTreeImplTestEvent> events = new ArrayBlockingQueue<HashTreeImplTestEvent>(1000);
        Storage storage = new StorageImplTest();
        HashTreeImplTestObj hTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                            htStorage,
                                                            storage,
                                                            events);
        HashTreeSyncManagerImpl syncManager = new HashTreeSyncManagerImpl(hTree,
                                                                          treeIdProvider,
                                                                          "localhost",
                                                                          HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
                                                                          30 * 1000,
                                                                          3000000,
                                                                          10);
        syncManager.init();
        waitForTheEvent(events, HashTreeImplTestEvent.UPDATE_SEGMENT, 30000);
        syncManager.shutdown();
    }

    @Test
    public void testFullTreeUpdate() throws InterruptedException {
        HashTreeStorage htStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        BlockingQueue<HashTreeImplTestEvent> events = new ArrayBlockingQueue<HashTreeImplTestEvent>(1000);
        Storage storage = new StorageImplTest();
        HashTreeImplTestObj hTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                            htStorage,
                                                            storage,
                                                            events);
        HashTreeSyncManagerImpl syncManager = new HashTreeSyncManagerImpl(hTree,
                                                                          treeIdProvider,
                                                                          "localhost",
                                                                          HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
                                                                          30 * 1000,
                                                                          30000000,
                                                                          10);
        syncManager.init();
        waitForTheEvent(events, HashTreeImplTestEvent.UPDATE_FULL_TREE, 10000);
        syncManager.shutdown();
    }

    @Test
    public void testSynch() throws Exception {
        HashTreeStorage localHTStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage remoteHTStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        BlockingQueue<HashTreeImplTestEvent> localEvents = new ArrayBlockingQueue<HashTreeImplTestEvent>(1000);
        BlockingQueue<HashTreeImplTestEvent> remoteEvents = new ArrayBlockingQueue<HashTreeImplTestEvent>(1000);
        StorageImplTest localStorage = new StorageImplTest();
        HashTreeImplTestObj localHTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                 localHTStorage,
                                                                 localStorage,
                                                                 localEvents);
        localStorage.setHashTree(localHTree);
        StorageImplTest remoteStorage = new StorageImplTest();
        HashTreeImplTestObj remoteHTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  remoteHTStorage,
                                                                  remoteStorage,
                                                                  remoteEvents);
        remoteStorage.setHashTree(remoteHTree);
        localStorage.put(HashTreeImplTestUtils.randomByteBuffer(),
                         HashTreeImplTestUtils.randomByteBuffer());
        HashTreeSyncManagerImpl localSyncManager = new HashTreeSyncManagerImpl(localHTree,
                                                                               treeIdProvider,
                                                                               "localhost",
                                                                               HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
                                                                               3000,
                                                                               300,
                                                                               10);

        HashTreeSyncManagerImpl remoteSyncManager = new HashTreeSyncManagerImpl(remoteHTree,
                                                                                treeIdProvider,
                                                                                "localhost",
                                                                                7999,
                                                                                3000,
                                                                                300,
                                                                                10);

        localSyncManager.addTreeAndPortNoForSync("localhost", 7999);
        localSyncManager.addTreeToSyncList("localhost", 1);
        localSyncManager.init();
        remoteSyncManager.init();

        waitForTheEvent(localEvents, HashTreeImplTestEvent.SYNCH, 10000000000000L);
        waitForTheEvent(remoteEvents, HashTreeImplTestEvent.SYNCH_INITIATED, 10000);
        localSyncManager.shutdown();
        remoteSyncManager.shutdown();
    }
}
