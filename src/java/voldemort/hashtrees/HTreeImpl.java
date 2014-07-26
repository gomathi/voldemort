package voldemort.hashtrees;

import static voldemort.utils.ByteUtils.sha1;
import static voldemort.utils.ByteUtils.toHexString;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Uses a binary tree to store the segment hashes.
 * 
 * 1) Segment hashes and (Key, Hash) pairs are stored on the same storage
 * {@link HTreeStorage}
 * 
 */
public class HTreeImpl implements HTree {

    private final static int ROOT_NODE = 0;
    private final static int MAX_CAPACITY = 1 << 30;

    private final int maxInternalNodeId;
    private final int noOfSegments;
    private final HTreeStorage hTStorage;
    private final Storage storage;

    public HTreeImpl(final int noOfSegments, final HTreeStorage hTStroage, final Storage storage) {
        if(noOfSegments < 0)
            throw new IllegalArgumentException("noOfSegments can not be a negative value.");
        this.noOfSegments = noOfSegments > MAX_CAPACITY ? MAX_CAPACITY
                                                       : roundUpToPowerOf2(noOfSegments);
        this.hTStorage = hTStroage;
        this.storage = storage;
        maxInternalNodeId = getNoOfInternalNodes(noOfSegments) - 2;
    }

    @Override
    public void put(String key, String value) {
        int segId = getSegmentId(key);
        String digest = toHexString(sha1(value.getBytes()));
        hTStorage.putSegmentData(segId, key, digest);
        hTStorage.setDirtySegment(segId);
    }

    @Override
    public void remove(String key) {
        int segId = getSegmentId(key);
        hTStorage.deleteSegmentData(segId, key);
        hTStorage.setDirtySegment(segId);
    }

    @Override
    public void update(HTree remoteTree) {
        Collection<Integer> leafNodesToCheck = new ArrayList<Integer>();
        Collection<Integer> missingNodes = new ArrayList<Integer>();
        Collection<Integer> nodesToDelete = new ArrayList<Integer>();

        findDifferences(remoteTree, leafNodesToCheck, missingNodes, nodesToDelete);

        BatchUpdater batchUpdater = new BatchUpdater(1000, remoteTree);

        Collection<Integer> segsToCheck = getSegmentIdsFromLeafIds(leafNodesToCheck);
        syncSegments(segsToCheck, remoteTree, batchUpdater);

        remoteTree.deleteNodes(nodesToDelete);

    }

    private void findDifferences(HTree remoteTree,
                                 Collection<Integer> nodesToCheck,
                                 Collection<Integer> missingNodes,
                                 Collection<Integer> nodesToDelete) {
        PeekingIteratorImpl<SegmentHash> localItr = null, remoteItr = null;
        SegmentHash local, remote;

        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(ROOT_NODE);
        while(!pQueue.isEmpty()) {

            localItr = new PeekingIteratorImpl<SegmentHash>(getSegmentHashes(pQueue));
            remoteItr = new PeekingIteratorImpl<SegmentHash>(remoteTree.getSegmentHashes(pQueue));
            pQueue = new ArrayDeque<Integer>();
            while(localItr.hasNext() && remoteItr.hasNext()) {
                local = localItr.peek();
                remote = remoteItr.peek();

                if(local.getNodeId() == remote.getNodeId()) {
                    if(!local.getHash().equals(remote.getHash())) {
                        if(isLeafNode(local.getNodeId()))
                            nodesToCheck.add(local.getNodeId());
                        else
                            pQueue.addAll(getImmediateChildren(local.getNodeId()));

                    }
                    localItr.next();
                    remoteItr.next();
                } else if(local.getNodeId() < remote.getNodeId()) {
                    missingNodes.add(local.getNodeId());
                    localItr.next();
                } else {
                    nodesToDelete.add(remote.getNodeId());
                    remoteItr.next();
                }
            }
        }
        if(localItr != null && localItr.hasNext()) {
            missingNodes.add(localItr.peek().getNodeId());
        } else if(remoteItr != null && remoteItr.hasNext()) {
            nodesToDelete.add(remoteItr.peek().getNodeId());
        }
    }

    private void syncSegments(Collection<Integer> segIds,
                              HTree remoteTree,
                              BatchUpdater batchUpdater) {
        for(int segId: segIds)
            syncSegment(segId, remoteTree, batchUpdater);
    }

    private void syncSegment(int segId, HTree remoteTree, BatchUpdater batchUpdater) {
        PeekingIteratorImpl<SegmentData> localDataItr = new PeekingIteratorImpl<SegmentData>(getSegment(segId));
        PeekingIteratorImpl<SegmentData> remoteDataItr = new PeekingIteratorImpl<SegmentData>(remoteTree.getSegment(segId));
        SegmentData local, remote;
        List<String> keysToBeUpdated = new ArrayList<String>();
        List<String> keysToBeRemoved = new ArrayList<String>();
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
    public List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds) {
        return hTStorage.getSegmentHashes(nodeIds);
    }

    @Override
    public List<SegmentData> getSegment(int segId) {
        return hTStorage.getSegment(segId);
    }

    @Override
    public void rebuildHTree() {
        List<Integer> dirtySegmentBuckets = hTStorage.getDirtySegments();
        List<Integer> dirtyLeafNodes = rebuildLeaves(dirtySegmentBuckets);
        rebuildInternalNodes(dirtyLeafNodes);
        hTStorage.unsetDirtySegmens(dirtySegmentBuckets);
    }

    @Override
    public void batchSPut(Map<String, String> keyValuePairs) {
        for(Map.Entry<String, String> keyValuePair: keyValuePairs.entrySet())
            storage.put(keyValuePair.getKey(), keyValuePair.getValue());
    }

    @Override
    public void batchSRemove(List<String> keys) {
        for(String key: keys)
            storage.remove(key);
    }

    @Override
    public void deleteNodes(Collection<Integer> nodeIds) {
        hTStorage.deleteSegments(getSegmentIdsOf(nodeIds));
    }

    /**
     * Rebuilds the dirty segments, and updates the segment hashes of the
     * leaves.
     * 
     * @return, the nodes ids of leaves in the tree.
     */
    private List<Integer> rebuildLeaves(List<Integer> dirtySegments) {
        List<Integer> dirtyNodeIds = new ArrayList<Integer>();
        for(int dirtySegId: dirtySegments) {
            String digest = digestSegmentData(dirtySegId);
            if(!digest.isEmpty()) {
                int nodeId = getSegmentIdFromLeafId(dirtySegId);
                hTStorage.putSegmentHash(nodeId, digest);
                dirtyNodeIds.add(nodeId);
            }
        }
        return dirtyNodeIds;
    }

    private String digestSegmentData(final int segId) {
        List<SegmentData> dirtySegmentData = hTStorage.getSegment(segId);
        if(dirtySegmentData.size() == 0)
            return "";

        StringBuilder sb = new StringBuilder();
        for(SegmentData sd: dirtySegmentData)
            sb.append(sd.getValue() + "\n");

        String digest = toHexString(sha1(sb.toString().getBytes()));
        return digest;
    }

    private void rebuildInternalNodes(final List<Integer> nodeIds) {
        Set<Integer> parentNodeIds = new TreeSet<Integer>();
        while(!nodeIds.isEmpty()) {
            for(int dirtyNodeId: nodeIds) {
                parentNodeIds.add(getParent(dirtyNodeId));
            }
            updateInternalNodes(parentNodeIds);

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
    private void updateInternalNodes(final Set<Integer> parentIds) {
        List<SegmentHash> segmentHashes;
        StringBuilder sb = new StringBuilder();
        for(int parentId: parentIds) {
            segmentHashes = hTStorage.getSegmentHashes(getImmediateChildren(parentId));
            for(SegmentHash sh: segmentHashes)
                sb.append(sh.getHash() + "\n");
            String digest = toHexString(sha1(sb.toString().getBytes()));
            hTStorage.putSegmentHash(parentId, digest);
            sb.setLength(0);
        }
    }

    private int getSegmentId(String key) {
        int hcode = key.hashCode();
        return hcode & noOfSegments;
    }

    /**
     * Segment block id starts with 0. Each leaf node corresponds to a segment
     * block. This function does the mapping from leaf node id to segment block
     * id.
     * 
     * @param leafNodeId
     * @return
     */
    private int getSegmentIdFromLeafId(final int leafNodeId) {
        return leafNodeId - maxInternalNodeId;
    }

    private Collection<Integer> getSegmentIdsFromLeafIds(final Collection<Integer> leafNodeIds) {
        return Collections2.transform(leafNodeIds, new Function<Integer, Integer>() {

            @Override
            public Integer apply(Integer leafNodeId) {
                return getSegmentIdFromLeafId(leafNodeId);
            }

        });
    }

    /**
     * 
     * @param nodeId, id of the internal node in the tree.
     * @return
     */
    private boolean isLeafNode(int nodeId) {
        return nodeId > maxInternalNodeId;
    }

    /**
     * Given a parent id, finds all the leaves that can be reached from the
     * parent.
     * 
     * @param pId, parent id
     * @return
     */
    private Collection<Integer> getAllLeafNodeIds(final int pId) {
        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(pId);
        while(pQueue.peek() <= maxInternalNodeId) {
            int cNodeId = pQueue.remove();
            pQueue.addAll(getImmediateChildren(cNodeId));
        }
        return pQueue;
    }

    /**
     * Given a collection of internal tree node ids, returns all the segment ids
     * which can be reached from these nodes.
     * 
     * @param nodeIds
     * @return, segment ids.
     */
    private Collection<Integer> getSegmentIdsOf(Collection<Integer> nodeIds) {
        Collection<Integer> leafNodeIds = new ArrayList<Integer>();
        for(int nodeId: nodeIds) {
            if(isLeafNode(nodeId)) {
                leafNodeIds.add(nodeId);
            } else {
                leafNodeIds.addAll(getAllLeafNodeIds(nodeId));
            }
        }

        return getSegmentIdsFromLeafIds(leafNodeIds);
    }

    private static int roundUpToPowerOf2(int number) {
        return number >= MAX_CAPACITY ? MAX_CAPACITY
                                     : (number > 1) ? Integer.highestOneBit((number - 1) << 1) : 1;
    }

    /**
     * Finds the count of internal nodes, given the no of leaf nodes.
     * 
     * @param noOfLeaves
     * @return
     */
    private static int getNoOfInternalNodes(final int noOfLeaves) {
        int result = ((int) Math.pow(2, height(noOfLeaves)));
        return result;
    }

    /**
     * Calculates the height of the tree, given the no of leaves.
     * 
     * @param noOfLeaves
     * @return
     */
    private static int height(int noOfLeaves) {
        int height = 0;
        while(noOfLeaves > 0) {
            noOfLeaves /= 2;
            height++;
        }
        return height;
    }

    /**
     * Returns the parent node id.
     * 
     * @param childId
     * @return
     */
    private static int getParent(final int childId) {
        if(childId <= 2)
            return 0;
        return (childId % 2 == 0) ? ((childId / 2) - 1) : (childId / 2);
    }

    /**
     * Finds the internal nodes that can be reached directly from the parent.
     * 
     * @param parentId
     * @return
     */
    private static List<Integer> getImmediateChildren(final int parentId) {
        List<Integer> children = new ArrayList<Integer>(2);
        for(int i = 1; i <= 2; i++) {
            children.add((2 * parentId) + i);
        }
        return children;
    }

}
