package voldemort.hashtrees;

import java.util.List;

/**
 * Defines Hash tree methods. Hash tree provides a way for nodes to synch up
 * quickly by exchanging very little information.
 * 
 */
public interface HTree {

    /**
     * 
     * @param key
     * @param value
     */
    void put(String key, String value);

    /**
     * Given an another HTree object, this method updates its HTree, based on
     * the difference in the keys.
     * 
     * @param htree
     */
    void synchWith(HTree htree);

    /**
     * Hash tree internal nodes store the hash of their leaf nodes. Given a set
     * of internal node ids, this returns the hashes that are stored on the
     * internal node.
     * 
     * @param nodeIds, internal tree node ids.
     * @return
     */
    List<SegmentHash> getSegmentHashes(List<Integer> nodeIds);

    /**
     * Hash tree data is stored on the leaf blocks. Given a segment id this
     * method is supposed to return (key,hash) pairs.
     * 
     * @param segId, id of the segment block.
     * @return
     */
    List<SegmentData> getSegmentBlock(int segId);

    /**
     * Hash tree implementations usually do not update the tree on every key
     * change. Rather, hash tree is rebuilt at every regular intervals. This
     * function provides an option to make a force call to update the tree.
     */
    void rebuildHTree();
}
