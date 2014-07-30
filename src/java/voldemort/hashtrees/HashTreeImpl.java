package voldemort.hashtrees;

import static voldemort.utils.ByteUtils.sha1;
import static voldemort.utils.TreeUtils.getImmediateChildren;
import static voldemort.utils.TreeUtils.getNoOfNodes;
import static voldemort.utils.TreeUtils.getParent;
import static voldemort.utils.TreeUtils.height;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.tasks.BGRebuildEntireTreeTask;
import voldemort.hashtrees.tasks.BGRebuildSegmentTreeTask;
import voldemort.hashtrees.tasks.Stoppable;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

/**
 * HashTree has the following components
 * 
 * 1) Segments, where the (key, hash of value) pairs are stored. All the pairs
 * are stored in sorted order. Whenever a key addition/removal happens on the
 * node, HashTree segment is updated. Keys are distributed using uniform hash
 * distribution. Max no of segments is {@link #MAX_NO_OF_BUCKETS}.
 * 
 * 2) A complete tree, where the segments' hashes are updated and maintained.
 * Tree is not updated on every update on a segment. Rather, tree update is
 * happening at regular intervals.Tree can be binary or 4-ary tree.
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

    // In milliseconds
    private final static long REBUILD_SEG_TIME_INTERVAL = 2 * 60 * 1000;
    private final static long REBUILD_HTREE_TIME_INTERVAL = 30 * 60 * 1000;
    private final static long REMOTE_TREE_SYNCH_INTERVAL = 5 * 60 * 1000;

    private final int noOfChildrenPerParent;
    private final int maxInternalNodeId;
    private final int noOfSegments;

    private final HashTreeStorage hTStorage;
    private final Storage storage;
    private final HashTreeIdProvider treeIdProvider;

    private final ExecutorService executors;
    private final ScheduledExecutorService scheduledExecutors;
    // Background tasks.
    private final BGSegmentDataUpdater bgSegDataUpdater;
    private final BGSynchTask bgSyncTask;
    private final BGRebuildEntireTreeTask bgRebuildTreeTask;
    private final BGRebuildSegmentTreeTask bgSegmentTreeTask;

    // A latch that is used internally while shutting down. Shutdown operation
    // is waiting on this latch for all other threads to finish up their work.
    private final CountDownLatch shutdownLatch;

    private static enum HTOperation {
        PUT,
        REMOVE
    }

    public HashTreeImpl(int noOfSegments,
                        int noOfChildrenPerParent,
                        final HashTreeStorage hTStroage,
                        final HashTreeIdProvider treeIdProvider,
                        final Storage storage,
                        final ExecutorService executors) {
        this.noOfSegments = (noOfSegments > MAX_NO_OF_BUCKETS) || (noOfSegments < 0) ? MAX_NO_OF_BUCKETS
                                                                                    : roundUpToPowerOf2(noOfSegments);
        this.noOfChildrenPerParent = noOfChildrenPerParent;
        this.maxInternalNodeId = getNoOfNodes(height(this.noOfSegments, this.noOfChildrenPerParent) - 1,
                                              this.noOfChildrenPerParent);
        this.treeIdProvider = treeIdProvider;
        this.hTStorage = hTStroage;
        this.storage = storage;
        this.executors = executors;
        this.scheduledExecutors = Executors.newScheduledThreadPool(2);
        this.shutdownLatch = new CountDownLatch(3);

        this.bgSegDataUpdater = new BGSegmentDataUpdater();
        this.bgSyncTask = new BGSynchTask();
        this.bgRebuildTreeTask = new BGRebuildEntireTreeTask(shutdownLatch);
        this.bgSegmentTreeTask = new BGRebuildSegmentTreeTask(this, shutdownLatch);

        startBackgroundTasks();
    }

    public HashTreeImpl(final HashTreeStorage hTStorage,
                        final HashTreeIdProvider treeIdProvider,
                        final Storage storage,
                        final ExecutorService executors) {
        this(MAX_NO_OF_BUCKETS, FOUR_ARY_TREE, hTStorage, treeIdProvider, storage, executors);
    }

    @Override
    public void put(final ByteArray key, final ByteArray value) {
        List<ByteArray> second = new ArrayList<ByteArray>(2);
        second.add(key);
        second.add(value);
        bgSegDataUpdater.enque(new Pair<HashTreeImpl.HTOperation, List<ByteArray>>(HTOperation.PUT,
                                                                                   second));
    }

    @Override
    public void remove(final ByteArray key) {
        List<ByteArray> second = new ArrayList<ByteArray>(1);
        second.add(key);
        bgSegDataUpdater.enque(new Pair<HashTreeImpl.HTOperation, List<ByteArray>>(HTOperation.REMOVE,
                                                                                   second));
    }

    private void putInternal(final ByteArray key, final ByteArray value) {
        int segId = getSegmentId(key);
        ByteArray digest = new ByteArray(sha1(value.get()));
        hTStorage.putSegmentData(treeIdProvider.getTreeId(key), segId, key, digest);
        hTStorage.setDirtySegment(treeIdProvider.getTreeId(key), segId);
    }

    private void removeInternal(final ByteArray key) {
        int segId = getSegmentId(key);
        hTStorage.deleteSegmentData(treeIdProvider.getTreeId(key), segId, key);
        hTStorage.setDirtySegment(treeIdProvider.getTreeId(key), segId);
    }

    @Override
    public void update(int treeId, final HashTree remoteTree) {
        Collection<Integer> leafNodesToCheck = new ArrayList<Integer>();
        Collection<Integer> missingNodes = new ArrayList<Integer>();

        findDifferences(treeId, remoteTree, leafNodesToCheck, missingNodes);

        BatchUpdater batchUpdater = new BatchUpdater(1000, remoteTree);

        Collection<Integer> segsToCheck = getSegmentIdsFromLeafIds(leafNodesToCheck);
        syncSegments(treeId, segsToCheck, remoteTree, batchUpdater);

    }

    private void findDifferences(int treeId,
                                 HashTree remoteTree,
                                 Collection<Integer> nodesToCheck,
                                 Collection<Integer> missingNodes) {
        CollectionPeekingIterator<SegmentHash> localItr = null, remoteItr = null;
        SegmentHash local, remote;

        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(ROOT_NODE);
        while(!pQueue.isEmpty()) {

            localItr = new CollectionPeekingIterator<SegmentHash>(getSegmentHashes(treeId, pQueue));
            remoteItr = new CollectionPeekingIterator<SegmentHash>(remoteTree.getSegmentHashes(treeId,
                                                                                               pQueue));
            pQueue = new ArrayDeque<Integer>();
            while(localItr.hasNext() && remoteItr.hasNext()) {
                local = localItr.peek();
                remote = remoteItr.peek();

                if(local.getNodeId() == remote.getNodeId()) {
                    if(!local.getHash().equals(remote.getHash())) {
                        if(isLeafNode(local.getNodeId()))
                            nodesToCheck.add(local.getNodeId());
                        else
                            pQueue.addAll(getImmediateChildren(local.getNodeId(),
                                                               this.noOfChildrenPerParent));

                    }
                    localItr.next();
                    remoteItr.next();
                } else if(local.getNodeId() < remote.getNodeId()) {
                    missingNodes.add(local.getNodeId());
                    localItr.next();
                } else {
                    remoteItr.next();
                }
            }
        }
        if(localItr != null && localItr.hasNext()) {
            missingNodes.add(localItr.peek().getNodeId());
        }
    }

    private void syncSegments(int treeId,
                              Collection<Integer> segIds,
                              HashTree remoteTree,
                              BatchUpdater batchUpdater) {
        for(int segId: segIds)
            syncSegment(treeId, segId, remoteTree, batchUpdater);
    }

    private void syncSegment(int treeId, int segId, HashTree remoteTree, BatchUpdater batchUpdater) {
        CollectionPeekingIterator<SegmentData> localDataItr = new CollectionPeekingIterator<SegmentData>(getSegment(treeId,
                                                                                                                    segId));
        CollectionPeekingIterator<SegmentData> remoteDataItr = new CollectionPeekingIterator<SegmentData>(remoteTree.getSegment(treeId,
                                                                                                                                segId));
        SegmentData local, remote;
        List<ByteArray> keysToBeUpdated = new ArrayList<ByteArray>();
        List<ByteArray> keysToBeRemoved = new ArrayList<ByteArray>();
        while(localDataItr.hasNext() && remoteDataItr.hasNext()) {
            local = localDataItr.peek();
            remote = remoteDataItr.peek();

            int compRes = local.getKey().compareTo(remote.getKey());
            if(compRes == 0) {
                if(!local.getValue().equals(remote.getValue()))
                    keysToBeUpdated.add(local.getKey());
                localDataItr.next();
                remoteDataItr.next();
            } else if(compRes < 0) {
                keysToBeUpdated.add(local.getKey());
                localDataItr.next();
            } else {
                keysToBeRemoved.add(remote.getKey());
                remoteDataItr.next();
            }
        }
        while(localDataItr.hasNext())
            keysToBeUpdated.add(localDataItr.next().getKey());
        while(remoteDataItr.hasNext())
            keysToBeRemoved.add(remoteDataItr.next().getKey());
        batchUpdater.addKeys(keysToBeUpdated);
        batchUpdater.removeKeys(keysToBeRemoved);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, final Collection<Integer> nodeIds) {
        return hTStorage.getSegmentHashes(treeId, nodeIds);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return hTStorage.getSegment(treeId, segId);
    }

    @Override
    public void updateSegmentHashes() {
        List<Integer> treeIds = treeIdProvider.getAllTreeIds();
        for(int treeId: treeIds) {
            List<Integer> dirtySegmentBuckets = hTStorage.clearAndGetDirtySegments(treeId);
            List<Integer> dirtyLeafNodes = rebuildLeaves(treeId, dirtySegmentBuckets);
            rebuildInternalNodes(treeId, dirtyLeafNodes);
        }
    }

    @Override
    public void batchSPut(Map<ByteArray, ByteArray> keyValuePairs) {
        for(Map.Entry<ByteArray, ByteArray> keyValuePair: keyValuePairs.entrySet())
            storage.put(keyValuePair.getKey(), keyValuePair.getValue());
    }

    @Override
    public void batchSRemove(final List<ByteArray> keys) {
        for(ByteArray key: keys)
            storage.remove(key);
    }

    @Override
    public void addTreeToSyncList(String hostName, HashTree remoteTree) {
        bgSyncTask.add(hostName, remoteTree);
    }

    @Override
    public void removeTreeFromSyncList(String hostName) {
        bgSyncTask.remove(hostName);
    }

    /**
     * Rebuilds the dirty segments, and updates the segment hashes of the
     * leaves.
     * 
     * @return, the nodes ids of leaves in the tree.
     */
    private List<Integer> rebuildLeaves(int treeId, final List<Integer> dirtySegments) {
        List<Integer> dirtyNodeIds = new ArrayList<Integer>();
        for(int dirtySegId: dirtySegments) {
            ByteArray digest = digestSegmentData(treeId, dirtySegId);
            int nodeId = getSegmentIdFromLeafId(dirtySegId);
            hTStorage.putSegmentHash(treeId, nodeId, digest);
            dirtyNodeIds.add(nodeId);
        }
        return dirtyNodeIds;
    }

    private ByteArray digestSegmentData(int treeId, int segId) {
        List<SegmentData> dirtySegmentData = hTStorage.getSegment(treeId, segId);

        StringBuilder sb = new StringBuilder();
        for(SegmentData sd: dirtySegmentData)
            sb.append(sd.getValue() + "\n");

        return new ByteArray(sha1(sb.toString().getBytes()));
    }

    /**
     * Updates the segment hashes iteratively for each level on the tree.
     * 
     * @param nodeIds
     */
    private void rebuildInternalNodes(int treeId, final List<Integer> nodeIds) {
        Set<Integer> parentNodeIds = new TreeSet<Integer>();
        while(!nodeIds.isEmpty()) {
            for(int nodeId: nodeIds) {
                parentNodeIds.add(getParent(nodeId, this.noOfChildrenPerParent));
            }
            updateInternalNodes(treeId, parentNodeIds);

            nodeIds.clear();
            nodeIds.addAll(parentNodeIds);
        }
    }

    /**
     * For each parent id, gets all the child hashes, and updates the parent
     * hash.
     * 
     * @param parentIds
     */
    private void updateInternalNodes(int treeId, final Set<Integer> parentIds) {
        List<SegmentHash> segmentHashes;
        StringBuilder sb = new StringBuilder();
        for(int parentId: parentIds) {
            segmentHashes = hTStorage.getSegmentHashes(treeId,
                                                       getImmediateChildren(parentId,
                                                                            this.noOfChildrenPerParent));
            for(SegmentHash sh: segmentHashes)
                sb.append(sh.getHash() + "\n");
            ByteArray digest = new ByteArray(sha1(sb.toString().getBytes()));
            hTStorage.putSegmentHash(treeId, parentId, digest);
            sb.setLength(0);
        }
    }

    private int getSegmentId(ByteArray key) {
        int hcode = key.hashCode();
        return hcode & (noOfSegments - 1);
    }

    /**
     * Segment block id starts with 0. Each leaf node corresponds to a segment
     * block. This function does the mapping from leaf node id to segment block
     * id.
     * 
     * @param leafNodeId
     * @return
     */
    private int getSegmentIdFromLeafId(int leafNodeId) {
        return leafNodeId - maxInternalNodeId;
    }

    private Collection<Integer> getSegmentIdsFromLeafIds(final Collection<Integer> leafNodeIds) {
        List<Integer> result = new ArrayList<Integer>(leafNodeIds.size());
        for(Integer leafNodeId: leafNodeIds)
            result.add(getSegmentIdFromLeafId(leafNodeId));
        return result;
    }

    /**
     * 
     * @param nodeId, id of the internal node in the tree.
     * @return
     */
    private boolean isLeafNode(int nodeId) {
        return nodeId > maxInternalNodeId;
    }

    private static int roundUpToPowerOf2(int number) {
        return number >= MAX_NO_OF_BUCKETS ? MAX_NO_OF_BUCKETS
                                          : (number > 1) ? Integer.highestOneBit((number - 1) << 1)
                                                        : 1;
    }

    /**
     * A task to enable non blocking calls on all
     * {@link HashTreeImpl#put(ByteArray, ByteArray)} and
     * {@link HashTreeImpl#remove(ByteArray)} operation.
     * 
     * This class provides a cleaner way to stop itself.
     */
    @Threadsafe
    private class BGSegmentDataUpdater implements Runnable, Stoppable {

        private final BlockingQueue<Pair<HTOperation, List<ByteArray>>> que = new ArrayBlockingQueue<Pair<HTOperation, List<ByteArray>>>(Integer.MAX_VALUE);
        private volatile boolean stopRequested = false;

        public void enque(Pair<HTOperation, List<ByteArray>> data) {
            if(stopRequested) {
                throw new IllegalStateException("Shut down is initiated. Unable to store the data.");
            }
            que.add(data);
        }

        @Override
        public void run() {
            for(;;) {
                try {
                    Pair<HTOperation, List<ByteArray>> pair = que.take();
                    switch(pair.getFirst()) {
                        case PUT:
                            putInternal(pair.getSecond().get(0), pair.getSecond().get(1));
                            break;
                        case REMOVE:
                            removeInternal(pair.getSecond().get(0));
                            break;
                    }
                } catch(InterruptedException e) {
                    // TODO Auto-generated catch block
                    logger.error("Interrupted while waiting for removing an element from the queue. Exiting");
                    return;
                } finally {
                    if(stopRequested && que.isEmpty()) {
                        shutdownLatch.countDown();
                        return;
                    }
                }
            }
        }

        @Override
        public void stop() {
            stopRequested = true;
        }
    }

    /**
     * This task resynchs given set of remote htree objects. This task can be
     * scheduled through the executor service.
     * 
     */
    @Threadsafe
    private class BGSynchTask implements Runnable, Stoppable {

        private final ConcurrentMap<String, HashTree> hostNameAndRemoteHTrees = new ConcurrentHashMap<String, HashTree>();

        public void add(String hostName, HashTree remoteHTree) {
            if(hostNameAndRemoteHTrees.putIfAbsent(hostName, remoteHTree) != null) {
                logger.debug(hostName + " is already present on the synch list. Skipping the host.");
                return;
            }
            logger.info(hostName + " is added to the synch list.");
        }

        public void remove(String hostName) {
            if(hostNameAndRemoteHTrees.remove(hostName) != null)
                logger.info(hostName + " is removed from synch list.");
        }

        @Override
        public void run() {

        }

        @Override
        public void stop() {}

    }

    private void startBackgroundTasks() {
        new Thread(bgSegDataUpdater).start();
        scheduledExecutors.scheduleWithFixedDelay(bgSyncTask,
                                                  0,
                                                  REMOTE_TREE_SYNCH_INTERVAL,
                                                  TimeUnit.MILLISECONDS);
        scheduledExecutors.scheduleWithFixedDelay(bgRebuildTreeTask,
                                                  0,
                                                  REBUILD_HTREE_TIME_INTERVAL,
                                                  TimeUnit.MILLISECONDS);
        scheduledExecutors.scheduleWithFixedDelay(bgSegmentTreeTask,
                                                  0,
                                                  REBUILD_SEG_TIME_INTERVAL,
                                                  TimeUnit.MILLISECONDS);
    }

    private void stopBackgroundTasks() {
        bgSegDataUpdater.stop();
        bgRebuildTreeTask.stop();
        bgSegmentTreeTask.stop();
    }

    /**
     * Provides an option to clean shutdown the background threads running on
     * this object.
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
    }

}
