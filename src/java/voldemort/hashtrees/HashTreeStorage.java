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
     * @param segId
     * @param key
     * @param digest
     */
    void putSegmentData(int segId, ByteArray key, ByteArray digest);

    /**
     * Deletes the given segement data from the block.
     * 
     * @param segId
     * @param key
     */
    void deleteSegmentData(int segId, ByteArray key);

    /**
     * Given a segment id, returns the list of all segment data in the
     * individual segment block.
     * 
     * @param segId
     * @return
     */
    List<SegmentData> getSegment(int segId);

    /**
     * Segment hash is the hash of all data inside a segment block. A segment
     * hash is stored on the tree node.
     * 
     * @param nodeId, identifier of the node in the hash tree.
     * @param digest
     */
    void putSegmentHash(int nodeId, ByteArray digest);

    /**
     * Returns the data inside the nodes of the hash tree. If the node id is not
     * present in the hash tree, the entry will be missing in the result.
     * 
     * @param nodeIds, internal tree node ids.
     * @return
     */
    List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds);

    /**
     * Marks a segment as a dirty.
     * 
     * @param segId
     */
    void setDirtySegment(int segId);

    /**
     * Returns all the ids of the dirty segments. Dirty markers are reset while
     * returning the result. This operation is atomic.
     * 
     * @return
     */
    List<Integer> clearAndGetDirtySegments();

}
