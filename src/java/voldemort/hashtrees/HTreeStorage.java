package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;

/**
 * Defines storage interface to be used the higher level HTree.
 * 
 */
public interface HTreeStorage {

    /**
     * A segment data is the value inside a segment block.
     * 
     * @param segId
     * @param key
     * @param digest
     */
    void putSegmentData(int segId, String key, String digest);

    /**
     * Deletes the given segement data from the block.
     * 
     * @param segId
     * @param key
     */
    void deleteSegmentData(int segId, String key);

    /**
     * Given a segment id, returns the list of all segment data in the
     * individual segment block.
     * 
     * @param segId
     * @return
     */
    List<SegmentData> getSegmentBlock(int segId);

    /**
     * Segment hash is the hash of all data inside a segment block. A segment
     * hash is stored on the tree node.
     * 
     * @param nodeId, identifier of the node in the hash tree.
     * @param digest
     */
    void putSegmentHash(int nodeId, String digest);

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
    void setDirtySegmentBlock(int segId);

    /**
     * Returns all the ids of the dirty segments.
     * 
     * @return
     */
    List<Integer> getDirtySegmentBlockIds();

    /**
     * Clears the dirty bits of the dirty segments.
     * 
     * @param dirtySegIds
     */
    void unsetDirtySegmentBlocks(Collection<Integer> dirtySegIds);

}
