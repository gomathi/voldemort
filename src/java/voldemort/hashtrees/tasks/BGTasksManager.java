package voldemort.hashtrees.tasks;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.HashTree;
import voldemort.hashtrees.HashTreeManager;

/**
 * Manages all the background threads like rebuilding segment hashes, rebuilding
 * segment trees and non-blocking segment data updater thread.
 * 
 */
public class BGTasksManager {

    private final static Logger LOG = Logger.getLogger(BGTasksManager.class);

    public final BGSegmentDataUpdater bgSegDataUpdater;
    public final BGHashTreeServer bgHashTreeServer;
    private volatile boolean tasksRunning;

    public BGTasksManager(final HashTree hashTree,
                          final HashTreeManager hashTreeManager,
                          int serverPortNo) throws TTransportException {
        this.bgSegDataUpdater = new BGSegmentDataUpdater(hashTreeManager);
        this.bgHashTreeServer = new BGHashTreeServer(hashTree, hashTreeManager, serverPortNo);
    }

    public void startBackgroundTasks() {
        if(tasksRunning)
            throw new IllegalStateException("Tasks are already running.");

        new Thread(bgSegDataUpdater).start();
        new Thread(bgHashTreeServer).start();
        tasksRunning = true;
        LOG.info("HashTree background tasks have been initiated.");
    }

    private synchronized CountDownLatch stopBackgroundTasks() {
        CountDownLatch shutdownLatch = new CountDownLatch(2);
        bgSegDataUpdater.stop(shutdownLatch);
        bgHashTreeServer.stop(shutdownLatch);
        return shutdownLatch;
    }

    /**
     * Provides an option to clean shutdown the background threads running on
     * this object.
     */
    public void safeShutdown() {
        CountDownLatch shutdownLatch = stopBackgroundTasks();
        LOG.info("Waiting for the shut down of background threads.");
        try {
            shutdownLatch.await();
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while waiting for the shut down of background threads.");
        }
        tasksRunning = false;
        LOG.info("HashTree background tasks has been stopped.");
    }
}