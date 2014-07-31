package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;

import voldemort.utils.ByteArray;

/**
 * Uses level db for storing segment data. Uses {@link HashTreeStorageInMemory}
 * for storing segment hashes.
 * 
 */

public class HashTreePersistentStorage implements HashTreeStorage {

    @Override
    public void putSegmentData(int treeId, int segId, ByteArray key, ByteArray digest) {}

    @Override
    public void deleteSegmentData(int treeId, int segId, ByteArray key) {}

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return null;
    }

    @Override
    public void putSegmentHash(int treeId, int nodeId, ByteArray digest) {}

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds) {
        return null;
    }

    @Override
    public void setDirtySegment(int treeId, int segId) {}

    @Override
    public List<Integer> clearAndGetDirtySegments(int treeId) {
        return null;
    }

    @Override
    public void deleteTree(int treeId) {}

}
