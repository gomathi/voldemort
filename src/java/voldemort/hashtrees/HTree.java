package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;

/**
 * Defines Hash tree methods. Hash tree provides a way for nodes to synch up
 * quickly by exchanging very little information.
 * 
 */
public interface HTree {

    /**
     * Adds the key, and digest of value to the segment block.
     * 
     * @param key
     * @param value
     */
    void put(String key, String value);

    /**
     * Adds the (key,value) pair to the original storage, and also puts (key,
     * digest) pair to the merkle tree. Intended to be used while synch
     * operation.
     * 
     * @param key
     * @param value
     */
    void addAndPut(String key, String value);

    /**
     * Updates the other HTree based on the differences with local objects.
     * 
     * This function should be running on primary to synch with other replicas,
     * and not the other way.
     * 
     * @param remoteTree
     */
    void update(HTree remoteTree);

    /**
     * Hash tree internal nodes store the hash of their children nodes. Given a
     * set of internal node ids, this returns the hashes that are stored on the
     * internal node.
     * 
     * @param nodeIds, internal tree node ids.
     * @return
     */
    List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds);

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
