package voldemort.hashtrees;

import static voldemort.utils.ByteUtils.sha1;
import static voldemort.utils.TreeUtils.getImmediateChildren;
import static voldemort.utils.TreeUtils.getNoOfNodes;
import static voldemort.utils.TreeUtils.getParent;
import static voldemort.utils.TreeUtils.height;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import scala.actors.threadpool.Arrays;
import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.CollectionPeekingIterator;
import voldemort.utils.Pair;

/**
 * HashTree has segment blocks and segment trees.
 * 
 * 1) Segment blocks, where the (key, hash of value) pairs are stored. All the
 * pairs are stored in sorted order. Whenever a key addition/removal happens on
 * the node, HashTree segment is updated. Keys are distributed using uniform
 * hash distribution. Max no of segments is {@link #MAX_NO_OF_BUCKETS}.
 * 
 * 2) Segment trees, where the segments' hashes are updated and maintained. Tree
 * is not updated on every update on a segment. Rather, tree update is happening
 * at regular intervals.Tree can be binary or 4-ary tree.
 * 
 * HashTree can host multiple hash trees. Each hash tree is differentiated by
 * hash tree id.
 * 
 * Uses {@link HashTreeStorage} for storing tree and segments.
 * {@link HashTreeStorageInMemory} provides in memory implementation of storing
 * entire tree and segments.
 * 
 * {@link #put(ByteArray, ByteArray)} and {@link #remove(ByteArray)} are non
 * blocking calls in order to avoid any latency issue to storage layer on every
 * update.
 * 
 */
@Threadsafe
public class HashTreeImpl implements HashTree {

    private final static Logger logger = Logger.getLogger(HashTreeImpl.class);

    private final static int ROOT_NODE = 0;
    private final static int MAX_NO_OF_BUCKETS = 1 << 30;
    private final static int FOUR_ARY_TREE = 4;

    private final int childrenCountPerParent;
    private final int internalNodesCount;
    private final int segmentsCount;

    private final HashTreeStorage hTStorage;
    private final HashTreeIdProvider treeIdProvider;
    private final SegmentIdProvider segIdProvider;
    private final Storage storage;

    private final boolean enabledBGTasks;
    private final BGTasksManager bgTasksMgr;
    private final ConcurrentMap<Integer, ReentrantLock> treeLocks = new ConcurrentHashMap<Integer, ReentrantLock>();

    public HashTreeImpl(int noOfSegments,
                        int noOfChildrenPerParent,
                        final HashTreeIdProvider treeIdProvider,
                        final SegmentIdProvider segIdProvider,
                        final HashTreeStorage hTStroage,
                        final Storage storage,
                        final int serverPortNo,
                        final ExecutorService executors) {
        this.segmentsCount = ((noOfSegments > MAX_NO_OF_BUCKETS) || (noOfSegments < 0)) ? MAX_NO_OF_BUCKETS
                                                                                       : roundUpToPowerOf2(noOfSegments);
        this.childrenCountPerParent = noOfChildrenPerParent;
        this.internalNodesCount = getNoOfNodes((height(this.segmentsCount,
                                                       this.childrenCountPerParent) - 1),
                                               this.childrenCountPerParent);
        this.treeIdProvider = treeIdProvider;
        this.segIdProvider = segIdProvider;
        this.hTStorage = hTStroage;
        this.storage = storage;
        this.bgTasksMgr = (executors == null) ? null : new BGTasksManager(executors, serverPortNo);
        this.enabledBGTasks = (executors == null) ? false : true;
    }

    /**
     * Launches the HashTree with nonblocking puts and removes, also
     * additionally launches background threads to update segments, rebuild the
     * entire tree, and to synch remote hashtrees at regular intervals.
     * 
     * @param hTStorage
     * @param treeIdProvider
     * @param storage
     * @param executors
     */
    public HashTreeImpl(final HashTreeIdProvider treeIdProvider,
                        final HashTreeStorage hTStorage,
                        final Storage storage,
                        final ExecutorService executors) {
        this(MAX_NO_OF_BUCKETS,
             FOUR_ARY_TREE,
             treeIdProvider,
             new DefaultSegIdProviderImpl(MAX_NO_OF_BUCKETS),
             hTStorage,
             storage,
             HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
             executors);
    }

    /**
     * Launches the HashTree, with no background threads to update the hash
     * tree. This is mainly used for unit tests.
     * 
     * @param hTStorage
     * @param treeIdProvider
     * @param segIdProvider
     * @param storage
     */
    public HashTreeImpl(int noOfSegments,
                        final HashTreeIdProvider treeIdProvider,
                        final SegmentIdProvider segIdProvider,
                        final HashTreeStorage hTStorage,
                        final Storage storage) {
        this(noOfSegments,
             FOUR_ARY_TREE,
             treeIdProvider,
             segIdProvider,
             hTStorage,
             storage,
             HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
             null);
    }

    // For unit tests
    public HashTreeImpl(int noOfSegments,
                        int noOfChildrenPerParent,
                        final HashTreeIdProvider treeIdProvider,
                        final HashTreeStorage hTStorage,
                        final Storage storage) {
        this(noOfSegments,
             noOfChildrenPerParent,
             treeIdProvider,
             new DefaultSegIdProviderImpl(noOfSegments),
             hTStorage,
             storage,
             HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
             null);
    }

    @Override
    public void hPut(final ByteBuffer key, final ByteBuffer value) {
        if(enabledBGTasks) {
            List<ByteBuffer> second = new ArrayList<ByteBuffer>(2);
            second.add(key);
            second.add(value);
            bgTasksMgr.bgSegDataUpdater.enque(new Pair<HTOperation, List<ByteBuffer>>(HTOperation.PUT,
                                                                                      second));
        } else
            putInternal(key, value);
    }

    @Override
    public void hRemove(final ByteBuffer key) {
        if(enabledBGTasks) {
            List<ByteBuffer> second = new ArrayList<ByteBuffer>(1);
            second.add(key);
            bgTasksMgr.bgSegDataUpdater.enque(new Pair<HTOperation, List<ByteBuffer>>(HTOperation.REMOVE,
                                                                                      second));
        } else
            removeInternal(key);
    }

    void putInternal(final ByteBuffer key, final ByteBuffer value) {
        int segId = segIdProvider.getSegmentId(key);
        ByteBuffer digest = ByteBuffer.wrap(sha1(value.array()));
        hTStorage.putSegmentData(treeIdProvider.getTreeId(key), segId, key, digest);
        hTStorage.setDirtySegment(treeIdProvider.getTreeId(key), segId);
    }

    void removeInternal(final ByteBuffer key) {
        int segId = segIdProvider.getSegmentId(key);
        hTStorage.deleteSegmentData(treeIdProvider.getTreeId(key), segId, key);
        hTStorage.setDirtySegment(treeIdProvider.getTreeId(key), segId);
    }

    public static String getHexString(ByteBuffer... values) {
        StringBuffer sb = new StringBuffer();
        for(ByteBuffer value: values) {
            sb.append(ByteUtils.toHexString(value.array()));
        }
        return sb.toString();
    }

    @Override
    public boolean synch(int treeId, final HashTreeSyncInterface.Iface remoteTree)
            throws TException {

        if(!isReadyForSynch(treeId) || !remoteTree.isReadyForSynch(treeId)) {
            logger.info("HashTree has not been initialized. Not doing the sync. Skipping the task");
            return false;
        }

        Collection<Integer> leafNodesToCheck = new ArrayList<Integer>();
        Collection<Integer> missingNodesInRemote = new ArrayList<Integer>();
        List<Integer> missingNodesInLocal = new ArrayList<Integer>();

        findDifferences(treeId,
                        remoteTree,
                        leafNodesToCheck,
                        missingNodesInRemote,
                        missingNodesInLocal);

        if(leafNodesToCheck.isEmpty() && missingNodesInLocal.isEmpty()
           && missingNodesInRemote.isEmpty())
            return false;

        Collection<Integer> segsToCheck = getSegmentIdsFromLeafIds(leafNodesToCheck);
        syncSegments(treeId, segsToCheck, remoteTree);

        Collection<Integer> missingSegsInRemote = getSegmentIdsFromLeafIds(getAllLeafNodeIds(missingNodesInRemote));
        updateRemoteTreeWithMissingSegments(treeId, missingSegsInRemote, remoteTree);

        remoteTree.deleteTreeNodes(treeId, missingNodesInLocal);
        return true;
    }

    private void findDifferences(int treeId,
                                 HashTreeSyncInterface.Iface remoteTree,
                                 Collection<Integer> nodesToCheck,
                                 Collection<Integer> missingNodesInRemote,
                                 Collection<Integer> missingNodesInLocal) throws TException {
        CollectionPeekingIterator<SegmentHash> localItr = null, remoteItr = null;
        SegmentHash local, remote;

        List<Integer> pQueue = new ArrayList<Integer>();
        pQueue.add(ROOT_NODE);
        while(!pQueue.isEmpty()) {

            localItr = new CollectionPeekingIterator<SegmentHash>(getSegmentHashes(treeId, pQueue));
            remoteItr = new CollectionPeekingIterator<SegmentHash>(remoteTree.getSegmentHashes(treeId,
                                                                                               pQueue));
            pQueue = new ArrayList<Integer>();
            while(localItr.hasNext() && remoteItr.hasNext()) {
                local = localItr.peek();
                remote = remoteItr.peek();

                if(local.getNodeId() == remote.getNodeId()) {
                    if(!Arrays.equals(local.getHash(), remote.getHash())) {
                        if(isLeafNode(local.getNodeId()))
                            nodesToCheck.add(local.getNodeId());
                        else
                            pQueue.addAll(getImmediateChildren(local.getNodeId(),
                                                               this.childrenCountPerParent));

                    }
                    localItr.next();
                    remoteItr.next();
                } else if(local.getNodeId() < remote.getNodeId()) {
                    missingNodesInRemote.add(local.getNodeId());
                    localItr.next();
                } else {
                    missingNodesInLocal.add(remote.getNodeId());
                    remoteItr.next();
                }
            }
        }
        while(localItr != null && localItr.hasNext()) {
            missingNodesInRemote.add(localItr.next().getNodeId());
        }
        while(remoteItr != null && remoteItr.hasNext()) {
            missingNodesInLocal.add(remoteItr.next().getNodeId());
        }
    }

    private void syncSegments(int treeId,
                              Collection<Integer> segIds,
                              HashTreeSyncInterface.Iface remoteTree) throws TException {
        for(int segId: segIds)
            syncSegment(treeId, segId, remoteTree);
    }

    private void syncSegment(int treeId, int segId, HashTreeSyncInterface.Iface remoteTree)
            throws TException {
        CollectionPeekingIterator<SegmentData> localDataItr = new CollectionPeekingIterator<SegmentData>(getSegment(treeId,
                                                                                                                    segId));
        CollectionPeekingIterator<SegmentData> remoteDataItr = new CollectionPeekingIterator<SegmentData>(remoteTree.getSegment(treeId,
                                                                                                                                segId));

        Map<ByteBuffer, ByteBuffer> kvsForAddition = new HashMap<ByteBuffer, ByteBuffer>();
        List<ByteBuffer> keysForeRemoval = new ArrayList<ByteBuffer>();

        SegmentData local, remote;
        while(localDataItr.hasNext() && remoteDataItr.hasNext()) {
            local = localDataItr.peek();
            remote = remoteDataItr.peek();

            int compRes = ByteUtils.compare(local.getKey(), remote.getKey());
            if(compRes == 0) {
                if(!Arrays.equals(local.getDigest(), remote.getDigest()))
                    kvsForAddition.put(ByteBuffer.wrap(local.getKey()),
                                       storage.get(ByteBuffer.wrap(local.getKey())));
                localDataItr.next();
                remoteDataItr.next();
            } else if(compRes < 0) {
                kvsForAddition.put(ByteBuffer.wrap(local.getKey()),
                                   storage.get(ByteBuffer.wrap(local.getKey())));
                localDataItr.next();
            } else {
                keysForeRemoval.add(ByteBuffer.wrap(remote.getKey()));
                remoteDataItr.next();
            }
        }
        while(localDataItr.hasNext()) {
            local = localDataItr.next();
            kvsForAddition.put(ByteBuffer.wrap(local.getKey()),
                               storage.get(ByteBuffer.wrap(local.getKey())));
        }
        while(remoteDataItr.hasNext())
            keysForeRemoval.add(ByteBuffer.wrap(remoteDataItr.next().getKey()));

        remoteTree.sPut(kvsForAddition);
        remoteTree.sRemove(keysForeRemoval);
    }

    private void updateRemoteTreeWithMissingSegments(int treeId,
                                                     Collection<Integer> segIds,
                                                     HashTreeSyncInterface.Iface remoteTree)
            throws TException {
        for(int segId: segIds) {
            final Map<ByteBuffer, ByteBuffer> keyValuePairs = new HashMap<ByteBuffer, ByteBuffer>();
            List<SegmentData> sdValues = getSegment(treeId, segId);
            for(SegmentData sd: sdValues)
                keyValuePairs.put(ByteBuffer.wrap(sd.getKey()),
                                  storage.get(ByteBuffer.wrap(sd.getKey())));
            remoteTree.sPut(keyValuePairs);
        }
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) {
        return hTStorage.getSegmentHash(treeId, nodeId);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, final List<Integer> nodeIds) {
        return hTStorage.getSegmentHashes(treeId, nodeIds);
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) {
        return hTStorage.getSegmentData(treeId, segId, key);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return hTStorage.getSegment(treeId, segId);
    }

    private boolean acquireTreeLock(int treeId, boolean waitForLock) {
        if(!treeLocks.containsKey(treeId)) {
            ReentrantLock lock = new ReentrantLock();
            treeLocks.putIfAbsent(treeId, lock);
        }
        Lock lock = treeLocks.get(treeId);
        if(waitForLock) {
            lock.lock();
            return true;
        }
        return lock.tryLock();
    }

    private void releaseTreeLock(int treeId) {
        treeLocks.get(treeId);
    }

    @Override
    public void updateHashTrees(boolean fullRebuild) {
        List<Integer> treeIds = treeIdProvider.getAllTreeIds();
        for(int treeId: treeIds) {
            boolean acquiredLock = fullRebuild ? acquireTreeLock(treeId, true)
                                              : acquireTreeLock(treeId, false);
            if(acquiredLock) {
                try {
                    updateHashTree(treeId, fullRebuild);
                } finally {
                    releaseTreeLock(treeId);
                }
            }
        }
    }

    @Override
    public void updateHashTree(int treeId, boolean fullRebuild) {
        List<Integer> dirtySegmentBuckets = null;
        long currentTs = System.currentTimeMillis();
        if(fullRebuild) {
            long lastRebuiltTs = hTStorage.getLastFullyTreeReBuiltTimestamp(treeId);
            if((lastRebuiltTs - currentTs) < BGTasksManager.EXP_INTERVAL_BW_REBUILDS) {
                logger.debug("HashTree was rebuilt recently. Not rebuilding again. Skipping the task.");
                return;
            }
            hTStorage.clearAllSegments(treeId);
        } else
            dirtySegmentBuckets = hTStorage.clearAndGetDirtySegments(treeId);

        Map<Integer, ByteBuffer> dirtyNodeAndDigestMap = (fullRebuild) ? (rebuildLeaves(treeId,
                                                                                        0,
                                                                                        MAX_NO_OF_BUCKETS))
                                                                      : (rebuildLeaves(treeId,
                                                                                       dirtySegmentBuckets));
        rebuildInternalNodes(treeId, dirtyNodeAndDigestMap);
        for(Map.Entry<Integer, ByteBuffer> dirtyNodeAndDigest: dirtyNodeAndDigestMap.entrySet())
            hTStorage.putSegmentHash(treeId,
                                     dirtyNodeAndDigest.getKey(),
                                     dirtyNodeAndDigest.getValue());
        if(fullRebuild)
            hTStorage.setLastFullyTreeBuiltTimestamp(treeId, currentTs);
        hTStorage.setLastHashTreeUpdatedTimestamp(treeId, currentTs);
    }

    @Override
    public void sPut(final Map<ByteBuffer, ByteBuffer> keyValuePairs) {
        for(Map.Entry<ByteBuffer, ByteBuffer> keyValuePair: keyValuePairs.entrySet())
            storage.put(keyValuePair.getKey(), keyValuePair.getValue());
    }

    @Override
    public void sRemove(final List<ByteBuffer> keys) {
        for(ByteBuffer key: keys)
            storage.remove(key);
    }

    @Override
    public void addTreeToSyncList(String hostName, int treeId) {
        if(enabledBGTasks)
            bgTasksMgr.bgSyncTask.add(hostName, treeId);
    }

    @Override
    public void removeTreeFromSyncList(String hostName, int treeId) {
        if(enabledBGTasks)
            bgTasksMgr.bgSyncTask.remove(hostName, treeId);
    }

    @Override
    public void deleteTreeNodes(int treeId, List<Integer> nodeIds) {
        List<Integer> segIds = getSegmentIdsFromLeafIds(getAllLeafNodeIds(nodeIds));
        for(int segId: segIds) {
            Iterator<SegmentData> segDataItr = getSegment(treeId, segId).iterator();
            while(segDataItr.hasNext()) {
                storage.remove(ByteBuffer.wrap(segDataItr.next().getKey()));
            }
        }
    }

    /**
     * Rebuilds all segments, and updates the segment hashes of the leaves.
     * 
     * @param treeId, hash tree id
     * @param startSegId, inclusive
     * @param endSegId, exclusive
     * @return, node ids, and uncommitted digest.
     */
    private Map<Integer, ByteBuffer> rebuildLeaves(int treeId, int fromSegId, int toSegId) {
        Map<Integer, ByteBuffer> dirtyNodeIdAndDigestMap = new HashMap<Integer, ByteBuffer>();
        for(int dirtySegId = fromSegId; dirtySegId < toSegId; dirtySegId++) {
            ByteBuffer digest = digestSegmentData(treeId, dirtySegId);
            int nodeId = getLeafIdFromSegmentId(dirtySegId);
            dirtyNodeIdAndDigestMap.put(nodeId, digest);
        }
        return dirtyNodeIdAndDigestMap;
    }

    /**
     * Rebuilds the dirty segments, and updates the segment hashes of the
     * leaves.
     * 
     * @return, node ids, and uncommitted digest.
     */
    private Map<Integer, ByteBuffer> rebuildLeaves(int treeId, final List<Integer> dirtySegments) {
        Map<Integer, ByteBuffer> dirtyNodeIdAndDigestMap = new HashMap<Integer, ByteBuffer>();
        for(int dirtySegId: dirtySegments) {
            ByteBuffer digest = digestSegmentData(treeId, dirtySegId);
            int nodeId = getLeafIdFromSegmentId(dirtySegId);
            dirtyNodeIdAndDigestMap.put(nodeId, digest);
        }
        return dirtyNodeIdAndDigestMap;
    }

    private ByteBuffer digestSegmentData(int treeId, int segId) {
        List<SegmentData> dirtySegmentData = hTStorage.getSegment(treeId, segId);

        StringBuilder sb = new StringBuilder();
        for(SegmentData sd: dirtySegmentData)
            sb.append(getHexString(sd.key, sd.digest) + "\n");

        return ByteBuffer.wrap(sha1(sb.toString().getBytes()));
    }

    /**
     * Updates the segment hashes iteratively for each level on the tree.
     * 
     * @param nodeIdAndDigestMap
     */
    private void rebuildInternalNodes(int treeId, final Map<Integer, ByteBuffer> nodeIdAndDigestMap) {
        Set<Integer> parentNodeIds = new TreeSet<Integer>();
        Set<Integer> nodeIds = new TreeSet<Integer>();
        nodeIds.addAll(nodeIdAndDigestMap.keySet());

        while(!nodeIds.isEmpty()) {
            for(int nodeId: nodeIds)
                parentNodeIds.add(getParent(nodeId, this.childrenCountPerParent));

            rebuildParentNodes(treeId, parentNodeIds, nodeIdAndDigestMap);

            nodeIds.clear();
            nodeIds.addAll(parentNodeIds);
            parentNodeIds.clear();

            if(nodeIds.contains(ROOT_NODE))
                break;
        }
    }

    /**
     * For each parent id, gets all the child hashes, and updates the parent
     * hash.
     * 
     * @param parentIds
     */
    private void rebuildParentNodes(int treeId,
                                    final Set<Integer> parentIds,
                                    Map<Integer, ByteBuffer> nodeIdAndDigestMap) {
        List<Integer> children;
        String hashString;
        SegmentHash segHash;
        StringBuilder sb = new StringBuilder();

        for(int parentId: parentIds) {
            children = getImmediateChildren(parentId, this.childrenCountPerParent);

            for(int child: children) {
                if(nodeIdAndDigestMap.containsKey(child))
                    hashString = getHexString(nodeIdAndDigestMap.get(child));
                else {
                    segHash = hTStorage.getSegmentHash(treeId, child);
                    hashString = (segHash == null) ? null : getHexString(segHash.hash);
                }
                if(hashString != null)
                    sb.append(hashString + "\n");
            }
            ByteBuffer digest = ByteBuffer.wrap(sha1(sb.toString().getBytes()));
            nodeIdAndDigestMap.put(parentId, digest);
            sb.setLength(0);
        }
    }

    /**
     * Segment block id starts with 0. Each leaf node corresponds to a segment
     * block. This function does the mapping from leaf node id to segment block
     * id.
     * 
     * @param segId
     * @return
     */
    private int getLeafIdFromSegmentId(int segId) {
        return internalNodesCount + segId;
    }

    /**
     * 
     * @param leafNodeId
     * @return
     */
    private int getSegmentIdFromLeafId(int leafNodeId) {
        return leafNodeId - internalNodesCount;
    }

    private List<Integer> getSegmentIdsFromLeafIds(final Collection<Integer> leafNodeIds) {
        List<Integer> result = new ArrayList<Integer>(leafNodeIds.size());
        for(Integer leafNodeId: leafNodeIds)
            result.add(getSegmentIdFromLeafId(leafNodeId));
        return result;
    }

    /**
     * Given a node id, finds all the leaves that can be reached from this node.
     * If the nodeId is a leaf node, then that will be returned as the result.
     * 
     * @param nodeId
     * @return, all ids of leaf nodes.
     */
    private Collection<Integer> getAllLeafNodeIds(int nodeId) {
        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(nodeId);
        while(pQueue.peek() < internalNodesCount) {
            int cNodeId = pQueue.remove();
            pQueue.addAll(getImmediateChildren(cNodeId, childrenCountPerParent));
        }
        return pQueue;
    }

    private Collection<Integer> getAllLeafNodeIds(Collection<Integer> nodeIds) {
        Collection<Integer> result = new ArrayList<Integer>();
        for(int nodeId: nodeIds) {
            result.addAll(getAllLeafNodeIds(nodeId));
        }
        return result;
    }

    /**
     * 
     * @param nodeId, id of the internal node in the tree.
     * @return
     */
    private boolean isLeafNode(int nodeId) {
        return nodeId >= internalNodesCount;
    }

    private static int roundUpToPowerOf2(int number) {
        return (number >= MAX_NO_OF_BUCKETS) ? MAX_NO_OF_BUCKETS
                                            : ((number > 1) ? Integer.highestOneBit((number - 1) << 1)
                                                           : 1);
    }

    public void startBGTasks() throws TTransportException {
        if(enabledBGTasks)
            bgTasksMgr.startBackgroundTasks();
    }

    public void shutdown() {
        if(enabledBGTasks) {
            bgTasksMgr.safeShutdown();
        }
    }

    /**
     * Defines the function to return the segId given the key.
     * 
     */
    public static interface SegmentIdProvider {

        int getSegmentId(ByteBuffer key);
    }

    private static class DefaultSegIdProviderImpl implements SegmentIdProvider {

        private final int noOfBuckets;

        public DefaultSegIdProviderImpl(int noOfBuckets) {
            this.noOfBuckets = noOfBuckets;
        }

        @Override
        public int getSegmentId(ByteBuffer key) {
            int hcode = key.hashCode();
            return hcode & (noOfBuckets - 1);
        }

    }

    /**
     * Manages all the background threads like rebuilding segment hashes,
     * rebuilding segment trees and non blocking segment data updater thread.
     * 
     */
    private class BGTasksManager {

        // Rebuild segment time interval, not full rebuild, but rebuild of dirty
        // segments, in milliseconds. Should be scheduled in shorter intervals.
        private final static long REBUILD_SEG_TIME_INTERVAL = 2 * 60 * 1000;
        // Rebuild of the complete tree. Should be scheduled in longer
        // intervals.
        private final static long REBUILD_HTREE_TIME_INTERVAL = 7 * 60 * 1000;
        private final static long REMOTE_TREE_SYNCH_INTERVAL = 5 * 60 * 1000;
        // Expected time interval between two consecutive tree full rebuilds.
        private final static long EXP_INTERVAL_BW_REBUILDS = 25 * 60 * 1000;

        private final ExecutorService executors;
        private final ScheduledExecutorService scheduledExecutors;
        // Background tasks.
        private final List<BGStoppableTask> bgTasks;
        private final BGSegmentDataUpdater bgSegDataUpdater;
        private final BGSynchTask bgSyncTask;

        // A latch that is used internally while shutting down. Shutdown
        // operation
        // is waiting on this latch for all other threads to finish up their
        // work.
        private volatile CountDownLatch shutdownLatch;
        private final int serverPortNo;

        public BGTasksManager(final ExecutorService executors, int serverPortNo) {

            this.executors = executors;
            this.scheduledExecutors = Executors.newScheduledThreadPool(2);

            this.serverPortNo = serverPortNo;
            this.bgTasks = new ArrayList<BGStoppableTask>();
            this.bgSegDataUpdater = new BGSegmentDataUpdater(HashTreeImpl.this, shutdownLatch);
            this.bgSyncTask = new BGSynchTask(HashTreeImpl.this, shutdownLatch);

            bgTasks.add(bgSegDataUpdater);
            bgTasks.add(bgSyncTask);
        }

        private void startBackgroundTasks() throws TTransportException {

            BGStoppableTask bgRebuildTreeTask = new BGRebuildEntireTreeTask(HashTreeImpl.this,
                                                                            shutdownLatch);
            BGStoppableTask bgSegmentTreeTask = new BGRebuildSegmentTreeTask(HashTreeImpl.this,
                                                                             shutdownLatch);
            BGHashTreeServer bgHashTreeServer = new BGHashTreeServer(HashTreeImpl.this,
                                                                     serverPortNo,
                                                                     shutdownLatch);
            bgTasks.add(bgRebuildTreeTask);
            bgTasks.add(bgSegmentTreeTask);
            bgTasks.add(bgHashTreeServer);

            shutdownLatch = new CountDownLatch(bgTasks.size());

            new Thread(bgSegDataUpdater).start();
            new Thread(bgHashTreeServer).start();

            scheduledExecutors.scheduleWithFixedDelay(bgSyncTask,
                                                      0,
                                                      REMOTE_TREE_SYNCH_INTERVAL,
                                                      TimeUnit.MILLISECONDS);
            scheduledExecutors.scheduleWithFixedDelay(bgRebuildTreeTask,
                                                      0,
                                                      REBUILD_HTREE_TIME_INTERVAL,
                                                      TimeUnit.MILLISECONDS);

            scheduledExecutors.scheduleWithFixedDelay(bgSegmentTreeTask,
                                                      new Random().nextInt(1000),
                                                      REBUILD_SEG_TIME_INTERVAL,
                                                      TimeUnit.MILLISECONDS);
        }

        private void stopBackgroundTasks() {
            for(BGStoppableTask task: bgTasks)
                task.stop();
        }

        /**
         * Provides an option to clean shutdown the background threads running
         * on this object.
         */
        public void safeShutdown() {
            stopBackgroundTasks();
            logger.info("Waiting for the shut down of background threads.");
            try {
                shutdownLatch.await();
                logger.info("Segment data updater has been shut down.");
            } catch(InterruptedException e) {
                // TODO Auto-generated catch block
                logger.warn("Interrupted while waiting for the shut down of background threads.");
            }
            executors.shutdownNow();
            scheduledExecutors.shutdownNow();
            logger.info("HashTree has shut down.");
        }
    }

    @Override
    public boolean isReadyForSynch(int treeId) {
        long currentTs = System.currentTimeMillis();
        long diff = currentTs - hTStorage.getLastHashTreeUpdatedTimestamp(treeId);
        return diff <= (2 * BGTasksManager.REBUILD_SEG_TIME_INTERVAL);
    }

    @Override
    public String ping() throws TException {
        return "ping";
    }
}
