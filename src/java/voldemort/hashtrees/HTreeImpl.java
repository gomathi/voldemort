package voldemort.hashtrees;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

/**
 * Uses a binary tree to store the segment hashes.
 * 
 * 1) Segment hashes and (Key, Hash) pairs are stored on the same storage
 * {@link HTreeStorage}
 * 
 */
public class HTreeImpl implements HTree {

    private final static int ROOT_NODE = 0;
    private final int maxInternalNodeId;
    private final int noOfSegments;
    private final HTreeStorage hTStorage;
    private final Storage storage;

    public HTreeImpl(final int noOfSegments, final HTreeStorage hTStroage, final Storage storage) {
        this.noOfSegments = noOfSegments;
        this.hTStorage = hTStroage;
        this.storage = storage;
        maxInternalNodeId = getNoOfInternalNodeIds(noOfSegments) - 2;
    }

    @Override
    public void put(String key, String value) {
        int segId = getSegmentId(key);
        String digest = digest(value);
        hTStorage.putSegmentData(segId, key, digest);
        hTStorage.setDirtySegmentBlock(segId);
    }

    @Override
    public void remove(String key) {
        int segId = getSegmentId(key);
        hTStorage.deleteSegmentData(segId, key);
        hTStorage.setDirtySegmentBlock(segId);
    }

    @Override
    public void update(HTree remoteTree) {
        List<Integer> blocksToCheck = new ArrayList<Integer>();
        List<Integer> missingBlocksInRemote = new ArrayList<Integer>();
        List<Integer> missingBlocksInLocal = new ArrayList<Integer>();
        PeekingIteratorImpl<SegmentHash> localItr = null, remoteItr = null;
        SegmentHash local, remote;
        BatchUpdater batchUpdater = new BatchUpdater(1000, remoteTree);

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
                            blocksToCheck.add(getSegmentBlockId(local.getNodeId()));
                        else
                            pQueue.addAll(getImmediateChildren(local.getNodeId()));

                    }
                    localItr.next();
                    remoteItr.next();
                } else if(local.getNodeId() < remote.getNodeId()) {
                    if(isLeafNode(local.getNodeId()))
                        missingBlocksInRemote.add(getSegmentBlockId(local.getNodeId()));
                    else
                        missingBlocksInRemote.addAll(getAllLeafNodeIds(local.getNodeId()));
                    localItr.next();
                } else {
                    if(isLeafNode(remote.getNodeId()))
                        missingBlocksInLocal.add(getSegmentBlockId(remote.getNodeId()));
                    else
                        missingBlocksInLocal.addAll(getAllLeafNodeIds(remote.getNodeId()));
                    remoteItr.next();
                }
            }
        }
        if(localItr != null && localItr.hasNext()) {
            if(isLeafNode(localItr.peek().getNodeId()))
                missingBlocksInRemote.add(localItr.peek().getNodeId());
            else
                missingBlocksInRemote.addAll(getAllLeafNodeIds(localItr.peek().getNodeId()));
        } else if(remoteItr != null && remoteItr.hasNext()) {
            if(isLeafNode(remoteItr.peek().getNodeId()))
                missingBlocksInLocal.add(remoteItr.peek().getNodeId());
            else
                missingBlocksInLocal.addAll(getAllLeafNodeIds(remoteItr.peek().getNodeId()));
        }

        for(int blockId: blocksToCheck)
            checkAndSynchSegmentBlock(blockId, remoteTree, batchUpdater);
    }

    private void checkAndSynchSegmentBlock(int segBlockId,
                                           HTree remoteTree,
                                           BatchUpdater batchUpdater) {
        PeekingIteratorImpl<SegmentData> localDataItr = new PeekingIteratorImpl<SegmentData>(getSegmentBlock(segBlockId));
        PeekingIteratorImpl<SegmentData> remoteDataItr = new PeekingIteratorImpl<SegmentData>(remoteTree.getSegmentBlock(segBlockId));
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
    public List<SegmentData> getSegmentBlock(int segId) {
        return hTStorage.getSegmentBlock(segId);
    }

    @Override
    public void rebuildHTree() {
        List<Integer> dirtyNodeIds = rebuildLeaves();
        rebuildInternalNodes(dirtyNodeIds);
        hTStorage.unsetDirtySegmentBlocks(dirtyNodeIds);
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

    /**
     * Rebuilds the dirty segments, and updates the segment hashes of the
     * leaves.
     * 
     * @return, the nodes ids of leaves in the tree.
     */
    private List<Integer> rebuildLeaves() {
        List<Integer> dirtySegments = hTStorage.getDirtySegmentBlockIds();
        List<Integer> dirtyNodeIds = new ArrayList<Integer>();
        for(int dirtySegId: dirtySegments) {
            String digest = digestSegmentData(dirtySegId);
            if(!digest.isEmpty()) {
                int nodeId = maxInternalNodeId + dirtySegId;
                hTStorage.putSegmentHash(nodeId, digest);
                dirtyNodeIds.add(nodeId);
            }
        }
        return dirtyNodeIds;
    }

    private String digestSegmentData(final int segId) {
        List<SegmentData> dirtySegmentData = hTStorage.getSegmentBlock(segId);
        if(dirtySegmentData.size() == 0)
            return "";

        StringBuilder sb = new StringBuilder();
        for(SegmentData sd: dirtySegmentData)
            sb.append(sd.getValue() + "\n");

        String digest = digest(sb.toString());
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
            String digest = digest(sb.toString());
            hTStorage.putSegmentHash(parentId, digest);
            sb.setLength(0);
        }
    }

    private int getSegmentId(String key) {
        return -1;
    }

    /**
     * Segment block id starts with 0. Each leaf node corresponds to a segment
     * block. To calculate the segment block id given a leaf node id, we need to
     * subtract maxInternalNodeId.
     * 
     * @param leafNodeId
     * @return
     */
    private int getSegmentBlockId(final int leafNodeId) {
        return leafNodeId - maxInternalNodeId;
    }

    private boolean isLeafNode(int nodeId) {
        return nodeId > maxInternalNodeId;
    }

    /**
     * Given a parent id, finds all the leaves that can be reached from the
     * parent.
     * 
     * @param parentId
     * @return
     */
    private Collection<Integer> getAllLeafNodeIds(int parentId) {
        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(parentId);
        while(pQueue.peek() <= maxInternalNodeId) {
            int cNodeId = pQueue.remove();
            pQueue.addAll(getImmediateChildren(cNodeId));
        }
        return pQueue;
    }

    private static String digest(String data) {
        return null;
    }

    /**
     * Finds the count of internal nodes, given the no of leaf nodes.
     * 
     * @param noOfLeaves
     * @return
     */
    private static int getNoOfInternalNodeIds(final int noOfLeaves) {
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
