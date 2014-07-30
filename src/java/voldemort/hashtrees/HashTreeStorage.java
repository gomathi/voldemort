package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;

import voldemort.utils.ByteArray;

/**
 * Defines storage interface to be used by higher level HTree.
 * 
 */
public interface HashTreeStorage {

    /**
     * A segment data is the value inside a segment block.
     * 
     * @param treeId
     * @param segId
     * @param key
     * @param digest
     */
    void putSegmentData(int treeId, int segId, ByteArray key, ByteArray digest);

    /**
     * Deletes the given segement data from the block.
     * 
     * @param treeId
     * @param segId
     * @param key
     */
    void deleteSegmentData(int treeId, int segId, ByteArray key);

    /**
     * Given a segment id, returns the list of all segment data in the
     * individual segment block.
     * 
     * @param treeId
     * @param segId
     * @return
     */
    List<SegmentData> getSegment(int treeId, int segId);

    /**
     * Segment hash is the hash of all data inside a segment block. A segment
     * hash is stored on a tree node.
     * 
     * @param treeId
     * @param nodeId, identifier of the node in the hash tree.
     * @param digest
     */
    void putSegmentHash(int treeId, int nodeId, ByteArray digest);

    /**
     * Returns the data inside the nodes of the hash tree. If the node id is not
     * present in the hash tree, the entry will be missing in the result.
     * 
     * @param treeId
     * @param nodeIds, internal tree node ids.
     * @return
     */
    List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds);

    /**
     * Marks a segment as a dirty.
     * 
     * @param treeId
     * @param segId
     */
    void setDirtySegment(int treeId, int segId);

    /**
     * Returns all the ids of the dirty segments. Dirty markers are reset while
     * returning the result. This operation is atomic.
     * 
     * @param treeId
     * @return
     */
    List<Integer> clearAndGetDirtySegments(int treeId);

}
