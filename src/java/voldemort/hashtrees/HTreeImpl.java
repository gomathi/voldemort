package voldemort.hashtrees;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    private final int leafNodeOffset;
    private final int noOfSegments;
    private final HTreeStorage hTStorage;

    public HTreeImpl(final int noOfSegments, final HTreeStorage hTStroage) {
        this.noOfSegments = noOfSegments;
        this.hTStorage = hTStroage;
        leafNodeOffset = getNoOfInternalNodeIds(noOfSegments) - 2;
    }

    @Override
    public void put(String key, String value) {
        int segId = getSegmentId(key);
        String digest = digest(value);
        hTStorage.putSegmentData(segId, key, digest);
    }

    @Override
    public void synch(HTree htree, boolean synchRemoteOrLocal) {
        if(synchRemoteOrLocal)
            synch(this, htree);
        else
            synch(htree, this);
    }

    /**
     * 
     * @param src
     * @param dest
     */
    private void synch(HTree src, HTree dest) {
        List<Integer> blocksToCheck = new ArrayList<Integer>();
        List<Integer> missingBlocks = new ArrayList<Integer>();
        SegmentHashIterator fItr, sItr;

        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(0); // 0 is the root node.
        while(!pQueue.isEmpty()) {

            fItr = new SegmentHashIterator(src.getSegmentHashes(pQueue));
            sItr = new SegmentHashIterator(dest.getSegmentHashes(pQueue));
            while(fItr.hasNext() && sItr.hasNext()) {

            }
        }
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

    private List<Integer> rebuildLeaves() {
        List<Integer> dirtySegments = hTStorage.getDirtySegmentBlockIds();
        List<Integer> dirtyNodeIds = new ArrayList<Integer>();
        for(int dirtySegId: dirtySegments) {
            String digest = digestSegmentData(dirtySegId);
            if(!digest.isEmpty()) {
                int nodeId = leafNodeOffset + dirtySegId;
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

    /**
     * Given a parent id, finds all the leaves that can be reached from the
     * parent.
     * 
     * @param parentId
     * @param maxLeafNodeId, needs additional parameter to figure out when to
     *        stop.
     * @return
     */
    private static Collection<Integer> getAllLeafNodeIds(int parentId, int maxLeafNodeId) {
        Queue<Integer> pQueue = new ArrayDeque<Integer>();
        pQueue.add(parentId);
        while(pQueue.peek() <= maxLeafNodeId) {
            int cNodeId = pQueue.remove();
            pQueue.addAll(getImmediateChildren(cNodeId));
        }
        return pQueue;
    }

}
