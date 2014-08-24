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
package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.tasks.BGTasksManager;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.utils.Pair;
import voldemort.utils.Triple;

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
 * START -> REBUILD -> SYNCH -> REBUILD
 * 
 * At any point time, the manager can be asked to shutdown, by requesting stop.
 * 
 */
public class HashTreeManager implements Runnable, HashTreeLocalSyncManager {

    private enum STATE {
        START,
        REBUILD,
        SYNCH
    }

    private final static Logger LOG = Logger.getLogger(HashTreeManager.class);
    // Expected time interval between two consecutive tree full rebuilds.
    public final static long DEFAULT_FULL_TREE_TIME_INTERVAL = 30 * 60 * 1000;
    // If local hash tree do not receive rebuilt confirmation from remote node,
    // then it will not synch remote node. To handle worst case scenarios, we
    // will synch remote node after a specific period of time.
    public final static long DEFAULT_MAX_UNSYNCED_TIME_INTERVAL = 15 * 60 * 1000;

    private final long rebuildFullTreeTimeInterval;
    private final HashTree hashTree;
    private final HashTreeIdProvider treeIdProvider;
    private final HashTreeStorage htStorage;
    private final String thisHostName;
    private final boolean enableVersionedData;

    private final ConcurrentMap<String, Integer> hostNameAndRemotePortNo = new ConcurrentHashMap<String, Integer>();
    private final ConcurrentMap<String, HashTreeSyncInterface.Iface> hostNameAndRemoteHTrees = new ConcurrentHashMap<String, HashTreeSyncInterface.Iface>();
    private final ConcurrentSkipListMap<Integer, ConcurrentSkipListSet<String>> hostNameAndTreeIdMap = new ConcurrentSkipListMap<Integer, ConcurrentSkipListSet<String>>();

    private final ConcurrentHashMap<Pair<String, Integer>, Long> unSyncedTimeIntervalMap = new ConcurrentHashMap<Pair<String, Integer>, Long>();
    private final ConcurrentHashMap<Pair<String, Integer>, Long> hostNameAndTimestampMap = new ConcurrentHashMap<Pair<String, Integer>, Long>();
    private final ConcurrentHashMap<Triple<String, Integer, Long>, Boolean> expRebuiltConfirmMap = new ConcurrentHashMap<Triple<String, Integer, Long>, Boolean>();

    private volatile STATE currState = STATE.START;
    private volatile boolean enabledBGTasks;
    private volatile BGTasksManager bgTasksMgr;

    public HashTreeManager(String thisHostName,
                           HashTree hashTree,
                           HashTreeIdProvider treeIdProvider,
                           HashTreeStorage htStorage,
                           boolean enableVersionedData) {
        this(thisHostName,
             DEFAULT_FULL_TREE_TIME_INTERVAL,
             hashTree,
             treeIdProvider,
             htStorage,
             enableVersionedData);
    }

    public HashTreeManager(String hostName,
                           long rebuildFullTreeTimeInterval,
                           HashTree hashTree,
                           HashTreeIdProvider treeIdProvider,
                           HashTreeStorage htStorage,
                           boolean enableVersionedData) {
        this.thisHostName = hostName;
        this.rebuildFullTreeTimeInterval = rebuildFullTreeTimeInterval;
        this.hashTree = hashTree;
        this.treeIdProvider = treeIdProvider;
        this.htStorage = htStorage;
        this.enableVersionedData = enableVersionedData;
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
        currState = getNextState(currState);
        switch(getNextState(currState)) {
            case REBUILD:
                rebuild();
                break;
            case SYNCH:
                synch();
                break;
            case START:
            default:
                break;
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

    public void rebuild(long tokenNo, int treeId, long expFullRebuildTimeInt) throws Exception {
        boolean fullRebuild = (System.currentTimeMillis() - hashTree.getLastFullyRebuiltTimeStamp(treeId)) > expFullRebuildTimeInt ? true
                                                                                                                                  : false;
        hashTree.rebuildHashTree(treeId, fullRebuild);
        HashTreeSyncInterface.Iface client = getHashTreeClient(thisHostName);
        client.postRebuildHashTreeResponse(thisHostName, tokenNo, treeId);
    }

    private void rebuild() {
        List<Integer> treeIds = treeIdProvider.getAllPrimaryTreeIds();
        for(int treeId: treeIds) {
            expRebuiltConfirmMap.remove(treeId);
            boolean fullRebuild;
            try {
                fullRebuild = (System.currentTimeMillis() - hashTree.getLastFullyRebuiltTimeStamp(treeId)) > rebuildFullTreeTimeInterval ? true
                                                                                                                                        : false;
                sendRequestForRebuild(treeId);
                hashTree.rebuildHashTree(treeId, fullRebuild);
            } catch(Exception e) {
                LOG.warn("Exception occurred while rebuilding.", e);
            }
        }
    }

    private void sendRequestForRebuild(int treeId) {
        for(String hostName: hostNameAndTreeIdMap.get(treeId)) {
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

    private void synch() {
        LOG.info("Synching remote hash trees.");
        for(int treeId: treeIdProvider.getAllPrimaryTreeIds()) {
            for(String hostName: hostNameAndTreeIdMap.get(treeIdProvider)) {
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
                    if((value == true)) {
                        synch(treeId, hostName);
                        unSyncedTimeIntervalMap.remove(hostNameAndTreeId);
                    } else if((System.currentTimeMillis() - unsyncedTime) > DEFAULT_MAX_UNSYNCED_TIME_INTERVAL) {
                        synch(treeId, hostName);
                        unSyncedTimeIntervalMap.remove(hostNameAndTreeId);
                    } else {
                        LOG.info("Did not receive confirmation from " + hostNameAndTreeId
                                 + " for the rebuilding. Not syncing the remote node.");
                    }
                } catch(Exception e) {
                    LOG.error("Exception occurred while doing synch.", e);
                }
            }
        }
        LOG.info("Synching remote hash trees. - Done");
    }

    private void synch(int treeId, String hostName) throws Exception {
        try {
            Pair<String, Integer> hostNameAndTreeId = Pair.create(hostName, treeId);
            LOG.info("Syncing " + hostNameAndTreeId);
            HashTree remoteTree = new HashTreeClient(getHashTreeClient(hostName));
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
                                                portNoObj == null ? HashTreeClientGenerator.getRemoteHashTreeClient(hostName)
                                                                 : HashTreeClientGenerator.getRemoteHashTreeClient(hostName,
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

    /**
     * 
     * @param serverPortNo
     * @param noOfBGThreads for better throughput, the value should be a maximum
     *        of 3x or greater than or equal to 2x. x is no of hash trees.
     * @throws TTransportException
     */
    public void startBGTasks(int serverPortNo) throws TTransportException {
        if(bgTasksMgr != null)
            throw new IllegalStateException("Background tasks initiated already.");

        bgTasksMgr = new BGTasksManager(hashTree, this, serverPortNo);
        startBGTasks(bgTasksMgr);
    }

    // Used by unit test.
    public void startBGTasks(final BGTasksManager bgTasksMgr) {
        this.bgTasksMgr = bgTasksMgr;
        bgTasksMgr.startBackgroundTasks();
        enabledBGTasks = true;
    }

    public void shutdown() {
        if(enabledBGTasks) {
            bgTasksMgr.safeShutdown();
        }
    }

    public void hPut(final ByteBuffer key, final ByteBuffer value) {
        List<ByteBuffer> second = new ArrayList<ByteBuffer>(2);
        second.add(key);
        second.add(value);
        bgTasksMgr.bgSegDataUpdater.enque(new Pair<HTOperation, List<ByteBuffer>>(HTOperation.PUT,
                                                                                  second));
    }

    public void hPut(List<ByteBuffer> input) throws Exception {
        hashTree.hPut(input.get(0), input.get(1));
        if(enableVersionedData)
            htStorage.putVersionedDataAddition(treeIdProvider.getTreeId(input.get(0)),
                                               input.get(0),
                                               input.get(1));
    }

    public void hRemove(List<ByteBuffer> input) throws Exception {
        hashTree.hRemove(input.get(0));
        if(enableVersionedData)
            htStorage.putVersionedDataRemoval(treeIdProvider.getTreeId(input.get(0)), input.get(0));
    }

    public void hRemove(final ByteBuffer key) {
        List<ByteBuffer> second = new ArrayList<ByteBuffer>(1);
        second.add(key);
        bgTasksMgr.bgSegDataUpdater.enque(new Pair<HTOperation, List<ByteBuffer>>(HTOperation.REMOVE,
                                                                                  second));
    }
}
