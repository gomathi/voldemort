package voldemort.hashtrees;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import voldemort.utils.ByteArray;

/**
 * Uses level db for storing segment data. Uses {@link HashTreeStorageInMemory}
 * for storing segment hashes.
 * 
 */

public class HashTreePersistentStorage implements HashTreeStorage {

    private final BitSet dirtySegments;

    public HashTreePersistentStorage(final int noOfSegments) {
        dirtySegments = new BitSet(noOfSegments);
    }

    @Override
    public void putSegmentData(int segId, ByteArray key, ByteArray digest) {}

    @Override
    public void deleteSegmentData(int segId, ByteArray key) {}

    @Override
    public List<SegmentData> getSegment(int segId) {
        return null;
    }

    @Override
    public void putSegmentHash(int nodeId, ByteArray digest) {}

    @Override
    public List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds) {
        return null;
    }

    @Override
    public void setDirtySegment(int segId) {}

    @Override
    public List<Integer> getDirtySegments() {
        return null;
    }

    @Override
    public void unsetDirtySegmens(Collection<Integer> dirtySegIds) {}

}
