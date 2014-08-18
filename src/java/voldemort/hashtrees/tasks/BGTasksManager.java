package voldemort.hashtrees.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.HashTree;

/**
 * Manages all the background threads like rebuilding segment hashes, rebuilding
 * segment trees and non blocking segment data updater thread.
 * 
 */
public class BGTasksManager {

    private final static Logger LOG = Logger.getLogger(BGTasksManager.class);

    // Rebuild segment time interval, not full rebuild, but rebuild of dirty
    // segments, in milliseconds. Should be scheduled in shorter intervals.
    public final static long DEFAULT_REBUILD_SEG_TIME_INTERVAL = 2 * 60 * 1000;
    // Expected time interval between two consecutive tree full rebuilds.
    public final static long DEFAULT_FULL_TREE_TIME_INTERVAL = 25 * 60 * 1000;
    public final static long DEFAULT_TREE_SYNCH_INTERVAL = 5 * 60 * 1000;
    public final static long DEFAULT_INITIAL_DELAY = 0;

    private final ExecutorService executors;
    private final ScheduledExecutorService scheduledExecutors;

    private final List<BGStoppableTask> bgTasks;
    public final BGSegmentDataUpdater bgSegDataUpdater;
    public final BGSynchTask bgSyncTask;

    private final int serverPortNo;
    private final HashTree hashTree;
    private volatile boolean tasksRunning;

    private final long rebuildSegTimeInterval;
    private final long rebuildFullTreeTimeInterval;
    private final long remoteTreeSynchInterval;
    private final long initialDelay;

    public BGTasksManager(final HashTree hashTree, final ExecutorService executors, int serverPortNo) {
        this(hashTree,
             executors,
             serverPortNo,
             DEFAULT_INITIAL_DELAY,
             DEFAULT_REBUILD_SEG_TIME_INTERVAL,
             DEFAULT_FULL_TREE_TIME_INTERVAL,
             DEFAULT_TREE_SYNCH_INTERVAL);
    }

    public BGTasksManager(final HashTree hashTree,
                          final ExecutorService executors,
                          int serverPortNo,
                          long initialDelay,
                          long rebuildFullTreeTimeInterval,
                          long rebuildSegTimeInterval,
                          long remoteTreeSynchInterval) {
        this.hashTree = hashTree;
        this.executors = executors;
        this.scheduledExecutors = Executors.newScheduledThreadPool(2);

        this.serverPortNo = serverPortNo;
        this.initialDelay = initialDelay;
        this.rebuildFullTreeTimeInterval = rebuildFullTreeTimeInterval;
        this.rebuildSegTimeInterval = rebuildSegTimeInterval;
        this.remoteTreeSynchInterval = remoteTreeSynchInterval;
        this.bgTasks = new ArrayList<BGStoppableTask>();
        this.bgSegDataUpdater = new BGSegmentDataUpdater(hashTree);
        this.bgSyncTask = new BGSynchTask(hashTree);

        bgTasks.add(bgSegDataUpdater);
        bgTasks.add(bgSyncTask);
    }

    public void startBackgroundTasks() throws TTransportException {
        if(tasksRunning)
            throw new IllegalStateException("Tasks are already running.");

        BGStoppableTask bgRebuildTreeTask = new BGRebuildEntireTreeTask(hashTree);
        BGStoppableTask bgSegmentTreeTask = new BGRebuildSegmentTreeTask(hashTree);
        BGHashTreeServer bgHashTreeServer = new BGHashTreeServer(hashTree, serverPortNo);
        bgTasks.add(bgRebuildTreeTask);
        bgTasks.add(bgSegmentTreeTask);
        bgTasks.add(bgHashTreeServer);

        new Thread(bgSegDataUpdater).start();
        new Thread(bgHashTreeServer).start();

        scheduledExecutors.scheduleWithFixedDelay(bgSyncTask,
                                                  initialDelay,
                                                  remoteTreeSynchInterval,
                                                  TimeUnit.MILLISECONDS);
        scheduledExecutors.scheduleWithFixedDelay(bgRebuildTreeTask,
                                                  initialDelay,
                                                  rebuildFullTreeTimeInterval,
                                                  TimeUnit.MILLISECONDS);

        scheduledExecutors.scheduleWithFixedDelay(bgSegmentTreeTask,
                                                  initialDelay,
                                                  rebuildSegTimeInterval,
                                                  TimeUnit.MILLISECONDS);

        tasksRunning = true;
        LOG.info("HashTree background tasks have been initiated.");
    }

    public long getRebuildFullTreeTimeInterval() {
        return rebuildFullTreeTimeInterval;
    }

    public synchronized void enableSynch() {
        bgSyncTask.stop();
    }

    public synchronized void disableSynch() {
        bgSyncTask.reset();
    }

    private synchronized CountDownLatch stopBackgroundTasks() {
        CountDownLatch shutdownLatch = new CountDownLatch(bgTasks.size());
        for(BGStoppableTask task: bgTasks)
            task.stop(shutdownLatch);
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
            LOG.info("Segment data updater has been shut down.");
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while waiting for the shut down of background threads.");
        }
        executors.shutdownNow();
        scheduledExecutors.shutdownNow();
        tasksRunning = false;
        LOG.info("HashTree background tasks has been stopped.");
    }
}