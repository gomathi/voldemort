/*
 * Copyright 2008-2014 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.hashtrees.synch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.core.HashTree;
import voldemort.hashtrees.core.HashTreeIdProvider;
import voldemort.hashtrees.core.HashTreeImpl;
import voldemort.hashtrees.core.TaskQueue;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.utils.Pair;
import voldemort.utils.Triple;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * A state machine based manager which runs background tasks to rebuild hash
 * trees, and synch remote hash trees.
 * 
 * HashTrees updates tree hashes at regular intervals, not on every update of
 * the key. Hence if two hash trees are continuously updating their hashes at
 * different intervals, the synch will always cause a mismatch even though
 * underlying data is same. We should avoid unnecessary network transfers. Thus
 * a state machine based approach is used where whenever the primary hash tree
 * rebuilds its hash tree, it requests the remote hash tree to rebuild the hash
 * tree as well.
 * 
 * HashTreeManager goes through the following states.
 * 
 * START -> REBUILD -> SYNCH -> REBUILD -> STOP. STOP state can be reached from
 * REBUILD or SYNCH state.
 * 
 * At any point time, the manager can be asked to shutdown, by requesting stop.
 * 
 * {@link HashTreeImpl} is a standalone class, it does not do automatically
 * build segments or any additional synch functionalities, this class provides
 * provides those functions.
 * 
 */
public class HashTreeSyncManagerImpl extends BGStoppableTask implements HashTreeSyncManager {

    private enum STATE {
        START,
        REBUILD,
        SYNCH
    }

    private final static int DEFAULT_NO_OF_BG_THREADS = 10;
    private final static Logger LOG = Logger.getLogger(HashTreeSyncManagerImpl.class);
    // Expected time interval between two consecutive tree full rebuilds.
    public final static long DEFAULT_FULL_REBUILD_TIME_INTERVAL = 30 * 60 * 1000;
    // If local hash tree do not receive rebuilt confirmation from remote node,
    // then it will not synch remote node. To handle worst case scenarios, we
    // will synch remote node after a specific period of time.
    public final static long DEFAULT_MAX_UNSYNCED_TIME_INTERVAL = 15 * 60 * 1000;
    // Synch and Rebuild events are executed alternatively. This specifies the
    // lapse between these two events.
    public final static long DEFAULT_INTERVAL_BW_SYNCH_AND_REBUILD = 5 * 60 * 1000;

    private final HashTree hashTree;
    private final HashTreeIdProvider treeIdProvider;
    private final String localHostName;
    private final int serverPortNo;
    private final int noOfBGThreads;
    private final long fullRebuildTimeInterval;
    private final long intBWSynchAndRebuild;

    private final ConcurrentMap<String, Integer> hostNameAndRemotePortNo = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, HashTreeSyncInterface.Iface> hostNameAndRemoteHTrees = new ConcurrentHashMap<String, HashTreeSyncInterface.Iface>();
    private final ConcurrentSkipListMap<Integer, ConcurrentSkipListSet<String>> hostNameAndTreeIdMap = new ConcurrentSkipListMap<Integer, ConcurrentSkipListSet<String>>();

    private final ConcurrentHashMap<Pair<String, Integer>, Long> unSyncedTimeIntervalMap = new ConcurrentHashMap<Pair<String, Integer>, Long>();
    private final ConcurrentHashMap<Pair<String, Integer>, Long> hostNameAndTimestampMap = new ConcurrentHashMap<Pair<String, Integer>, Long>();
    private final ConcurrentHashMap<Triple<String, Integer, Long>, Boolean> expRebuiltConfirmMap = new ConcurrentHashMap<Triple<String, Integer, Long>, Boolean>();
    private final ScheduledExecutorService scheduledExecutors = Executors.newScheduledThreadPool(1);
    private final ExecutorService fixedExecutors;

    private volatile STATE currState = STATE.START;
    private volatile boolean bgTasksEnabled;
    private volatile BGHashTreeServer bgHTServer;

    public HashTreeSyncManagerImpl(String thisHostName,
                                   HashTree hashTree,
                                   HashTreeIdProvider treeIdProvider,
                                   int serverPortNo) {
        this(hashTree,
             treeIdProvider,
             thisHostName,
             serverPortNo,
             DEFAULT_FULL_REBUILD_TIME_INTERVAL,
             DEFAULT_INTERVAL_BW_SYNCH_AND_REBUILD,
             DEFAULT_NO_OF_BG_THREADS);
    }

    public HashTreeSyncManagerImpl(HashTree hashTree,
                                   HashTreeIdProvider treeIdProvider,
                                   String localHostName,
                                   int serverPortNo,
                                   long fullRebuildTimeInterval,
                                   long intBWSynchAndRebuild,
                                   int noOfBGThreads) {
        this.hashTree = hashTree;
        this.treeIdProvider = treeIdProvider;
        this.localHostName = localHostName;
        this.fullRebuildTimeInterval = fullRebuildTimeInterval;
        this.noOfBGThreads = noOfBGThreads;
        this.serverPortNo = serverPortNo;
        this.intBWSynchAndRebuild = intBWSynchAndRebuild;
        this.fixedExecutors = Executors.newFixedThreadPool(noOfBGThreads);
    }

    private STATE getNextState(STATE currState) {
        switch(currState) {
            case START:
            case SYNCH:
                return STATE.REBUILD;
            case REBUILD:
                return STATE.SYNCH;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                currState = getNextState(currState);
                switch(currState) {
                    case REBUILD:
                        rebuildAllLocallyManagedTrees();
                        break;
                    case SYNCH:
                        synch();
                        break;
                    case START:
                    default:
                        break;
                }
            } finally {
                disableRunningStatus();
            }
        }
    }

    public void onRebuildHashTreeResponse(String hostName, long tokenNo, int treeId) {
        Triple<String, Integer, Long> triple = Triple.create(hostName, treeId, tokenNo);
        Boolean value = expRebuiltConfirmMap.get(triple);
        if(value != null) {
            value = expRebuiltConfirmMap.put(triple, true);
            if(value == null)
                expRebuiltConfirmMap.remove(triple);
        }
    }

    public void onRebuildHashTreeRequest(long tokenNo, int treeId, long expFullRebuildTimeInt)
            throws Exception {
        boolean fullRebuild = (System.currentTimeMillis() - hashTree.getLastFullyRebuiltTimeStamp(treeId)) > expFullRebuildTimeInt ? true
                                                                                                                                  : false;
        hashTree.rebuildHashTree(treeId, fullRebuild);
        HashTreeSyncInterface.Iface client = getHashTreeClient(localHostName);
        client.postRebuildHashTreeResponse(localHostName, tokenNo, treeId);
    }

    private void rebuildAllLocallyManagedTrees() {
        Collection<Integer> treeIds = treeIdProvider.getAllPrimaryTreeIds();
        if(treeIds.size() == 0) {
            LOG.info("There are no locally managed trees. So skipping rebuild operation.");
            return;
        }
        List<Pair<Integer, Boolean>> treeIdAndRebuildType = new ArrayList<Pair<Integer, Boolean>>();
        for(int treeId: treeIds) {
            expRebuiltConfirmMap.remove(treeId);
            boolean fullRebuild;
            try {
                fullRebuild = (System.currentTimeMillis() - hashTree.getLastFullyRebuiltTimeStamp(treeId)) > fullRebuildTimeInterval ? true
                                                                                                                                    : false;
                sendRequestForRebuild(treeId);
                treeIdAndRebuildType.add(Pair.create(treeId, fullRebuild));
            } catch(Exception e) {
                LOG.warn("Exception occurred while rebuilding.", e);
            }
        }
        Collection<Callable<Void>> rebuildTasks = Collections2.transform(treeIdAndRebuildType,
                                                                         new Function<Pair<Integer, Boolean>, Callable<Void>>() {

                                                                             @Override
                                                                             public Callable<Void> apply(final Pair<Integer, Boolean> input) {
                                                                                 return new Callable<Void>() {

                                                                                     @Override
                                                                                     public Void call()
                                                                                             throws Exception {
                                                                                         hashTree.rebuildHashTree(input.getFirst(),
                                                                                                                  input.getSecond());
                                                                                         return null;
                                                                                     }
                                                                                 };
                                                                             }
                                                                         });
        LOG.info("Building locally managed trees.");
        TaskQueue<Void> taskQueue = new TaskQueue<Void>(fixedExecutors,
                                                        rebuildTasks.iterator(),
                                                        noOfBGThreads);
        while(taskQueue.hasNext()) {
            taskQueue.next();
        }
        LOG.info("Building locally managed trees - Done");
    }

    private void sendRequestForRebuild(int treeId) {
        ConcurrentSkipListSet<String> hosts = hostNameAndTreeIdMap.get(treeId);
        if(hosts != null) {
            for(String hostName: hosts) {
                Pair<String, Integer> hostNameAndTreeId = Pair.create(hostName, treeId);
                try {
                    HashTreeSyncInterface.Iface client = getHashTreeClient(hostName);
                    long timestamp = System.currentTimeMillis();
                    Triple<String, Integer, Long> hostNameTreeIdAndTimestamp = Triple.create(hostName,
                                                                                             treeId,
                                                                                             timestamp);
                    client.rebuildHashTree(timestamp, treeId, DEFAULT_MAX_UNSYNCED_TIME_INTERVAL);
                    hostNameAndTimestampMap.put(hostNameAndTreeId, timestamp);
                    expRebuiltConfirmMap.put(hostNameTreeIdAndTimestamp, false);
                    unSyncedTimeIntervalMap.putIfAbsent(hostNameAndTreeId, timestamp);
                } catch(TTransportException e) {
                    LOG.warn("Unable to send rebuild notification to " + hostNameAndTreeId);
                } catch(TException e) {
                    LOG.warn("Unable to send rebuild notification to " + hostNameAndTreeId);
                }
            }
        }
    }

    private void synch() {
        Collection<Integer> treeIds = treeIdProvider.getAllPrimaryTreeIds();
        List<Pair<String, Integer>> hostNameAndTreeIdList = new ArrayList<Pair<String, Integer>>();

        for(int treeId: treeIds) {
            ConcurrentSkipListSet<String> hosts = hostNameAndTreeIdMap.get(treeId);
            if(hosts != null) {
                for(String hostName: hosts) {
                    Pair<String, Integer> hostNameAndTreeId = Pair.create(hostName, treeId);
                    Long timestamp = hostNameAndTimestampMap.remove(hostNameAndTreeId);
                    if(timestamp == null)
                        continue;
                    Boolean value = expRebuiltConfirmMap.remove(Triple.create(hostName,
                                                                              treeId,
                                                                              timestamp));
                    Long unsyncedTime = unSyncedTimeIntervalMap.get(hostNameAndTreeId);

                    if(value == null) {
                        LOG.warn("Rebuild notification entry is not available. Synch should be followed by rebuild. Skipping");
                        continue;
                    }
                    if(unsyncedTime == null) {
                        LOG.warn("Unsynced info entry is not available. Synch should be followed by rebuild. Skipping");
                        continue;
                    }

                    try {
                        if((value == true)
                           || ((System.currentTimeMillis() - unsyncedTime) > DEFAULT_MAX_UNSYNCED_TIME_INTERVAL)) {
                            hostNameAndTreeIdList.add(hostNameAndTreeId);
                            unSyncedTimeIntervalMap.remove(hostNameAndTreeId);
                        } else {
                            LOG.info("Did not receive confirmation from " + hostNameAndTreeId
                                     + " for the rebuilding. Not syncing the remote node.");
                        }
                    } catch(Exception e) {
                        LOG.error("Exception occurred while doing synch.", e);
                    }

                    if(hasStopRequested()) {
                        LOG.info("Stop has been requested. Skipping further sync tasks");
                        return;
                    }
                }
            }
        }

        if(hostNameAndTreeIdList.size() == 0) {
            LOG.info("There is no synch required for any remote trees. Skipping this cycle.");
            return;
        }
        LOG.info("Synching remote hash trees.");
        Collection<Callable<Void>> syncTasks = Collections2.transform(hostNameAndTreeIdList,
                                                                      new Function<Pair<String, Integer>, Callable<Void>>() {

                                                                          @Override
                                                                          public Callable<Void> apply(final Pair<String, Integer> input) {
                                                                              return new Callable<Void>() {

                                                                                  @Override
                                                                                  public Void call()
                                                                                          throws Exception {
                                                                                      synch(input.getSecond(),
                                                                                            input.getFirst());
                                                                                      return null;
                                                                                  }
                                                                              };
                                                                          }
                                                                      });

        TaskQueue<Void> taskQueue = new TaskQueue<Void>(fixedExecutors,
                                                        syncTasks.iterator(),
                                                        noOfBGThreads);
        while(taskQueue.hasNext()) {
            taskQueue.next();
        }
        LOG.info("Synching remote hash trees. - Done");
    }

    private void synch(int treeId, String hostName) throws Exception {
        try {
            Pair<String, Integer> hostNameAndTreeId = Pair.create(hostName, treeId);
            LOG.info("Syncing " + hostNameAndTreeId);
            HashTree remoteTree = new HashTreeClientImpl(getHashTreeClient(hostName));
            hashTree.synch(treeId, remoteTree);
            LOG.info("Syncing " + hostNameAndTreeId + " complete.");
        } catch(TException e) {
            LOG.warn("Unable to synch remote hash tree server : [" + hostName + "," + treeId + "]",
                     e);
        }
    }

    private HashTreeSyncInterface.Iface getHashTreeClient(String hostName)
            throws TTransportException {
        if(!hostNameAndRemoteHTrees.containsKey(hostName)) {
            Integer portNoObj = hostNameAndRemotePortNo.get(hostName);
            hostNameAndRemoteHTrees.putIfAbsent(hostName,
                                                portNoObj == null ? HashTreeClientProvider.getRemoteHashTreeClient(hostName)
                                                                 : HashTreeClientProvider.getRemoteHashTreeClient(hostName,
                                                                                                                  portNoObj));
        }
        return hostNameAndRemoteHTrees.get(hostName);
    }

    @Override
    public void addTreeAndPortNoForSync(String hostName, int portNo) {
        hostNameAndRemotePortNo.put(hostName, portNo);
    }

    @Override
    public void addTreeToSyncList(String hostName, int treeId) {
        hostNameAndTreeIdMap.putIfAbsent(treeId, new ConcurrentSkipListSet<String>());
        hostNameAndTreeIdMap.get(treeId).add(hostName);
        LOG.debug("Host " + hostName + " and treeId :" + treeId + " has been added to sync list.");
    }

    @Override
    public void removeTreeFromSyncList(String hostName, int treeId) {
        hostNameAndTreeIdMap.putIfAbsent(treeId, new ConcurrentSkipListSet<String>());
        hostNameAndTreeIdMap.get(treeId).remove(hostName);
        LOG.debug("Host " + hostName + " and treeId :" + treeId
                  + " has been removed from sync list.");
    }

    public synchronized void init() {
        if(bgTasksEnabled) {
            LOG.warn("Background tasks already started. Skipping.");
            return;
        }

        CountDownLatch initializedLatch = new CountDownLatch(1);
        if(bgHTServer == null)
            bgHTServer = new BGHashTreeServer(hashTree, this, serverPortNo, initializedLatch);
        new Thread(bgHTServer).start();
        try {
            initializedLatch.await();
        } catch(InterruptedException e) {
            LOG.warn("Exception occurred while waiting for the server to start", e);
        }
        scheduledExecutors.scheduleWithFixedDelay(this,
                                                  0,
                                                  intBWSynchAndRebuild,
                                                  TimeUnit.MILLISECONDS);
        bgTasksEnabled = true;
    }

    public void shutdown() {
        if(!bgTasksEnabled) {
            LOG.warn("Background tasks already stopped. Skipping.");
            return;
        }
        CountDownLatch localLatch = new CountDownLatch(1);
        super.stop(localLatch);
        try {
            localLatch.await();
        } catch(InterruptedException e) {
            LOG.warn("Exception occurred while stopping the operations.", e);
        }
        bgHTServer.stop();
        scheduledExecutors.shutdownNow();
    }
}
