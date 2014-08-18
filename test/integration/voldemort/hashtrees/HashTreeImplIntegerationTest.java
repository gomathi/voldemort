package voldemort.hashtrees;

import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_HTREE_SERVER_PORT_NO;
import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_SEG_DATA_BLOCKS_COUNT;
import static voldemort.hashtrees.HashTreeImplTestUtils.DEFAULT_TREE_ID;
import static voldemort.hashtrees.HashTreeImplTestUtils.generateInMemoryStore;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import voldemort.hashtrees.HashTreeImplTestUtils.StorageImplTest;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.Storage;
import voldemort.hashtrees.tasks.BGTasksManager;

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
    public void testSynch() throws TException, InterruptedException {
        HashTreeStorage localHTStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage remoteHTStorage = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        BlockingQueue<HashTreeImplEvent> localEvents = new ArrayBlockingQueue<HashTreeImplEvent>(10);
        BlockingQueue<HashTreeImplEvent> remoteEvents = new ArrayBlockingQueue<HashTreeImplEvent>(10);
        Storage localStorage = new StorageImplTest();
        Storage remoteStorage = new StorageImplTest();
        HashTreeImplTestObj localHTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                 localHTStorage,
                                                                 localStorage,
                                                                 localEvents);
        BGTasksManager localBGTasks = new BGTasksManager(localHTree,
                                                         Executors.newFixedThreadPool(10),
                                                         DEFAULT_HTREE_SERVER_PORT_NO,
                                                         1000,
                                                         30000,
                                                         30000,
                                                         1000);
        HashTreeImplTestObj remoteHTree = new HashTreeImplTestObj(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  remoteHTStorage,
                                                                  remoteStorage,
                                                                  remoteEvents);
        BGTasksManager remoteBGTasks = new BGTasksManager(remoteHTree,
                                                          Executors.newFixedThreadPool(10),
                                                          22222,
                                                          1000,
                                                          30000,
                                                          30000,
                                                          1000);

        localHTree.startBGTasks(localBGTasks);
        localHTree.addTreeAndPortNoForSync("localhost", 22222);
        localHTree.addTreeToSyncList("localhost", DEFAULT_TREE_ID);

        remoteHTree.startBGTasks(remoteBGTasks);
        waitForTheEvent(localEvents, HashTreeImplEvent.SYNCH, 10000);
        waitForTheEvent(remoteEvents, HashTreeImplEvent.SYNCH_INITIATED, 10000);
        localHTree.shutdown();
        remoteHTree.shutdown();
    }

}
