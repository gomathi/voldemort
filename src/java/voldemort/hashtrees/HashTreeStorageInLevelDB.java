package voldemort.hashtrees;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;

import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

/**
 * Uses level db for storing segment data. Uses {@link HashTreeStorageInMemory}
 * for storing segment hashes.
 * 
 */

public class HashTreeStorageInLevelDB implements HashTreeStorage {

    private final BitSet dirtySegments;

    public HashTreeStorageInLevelDB(final int noOfSegments) {
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
    public void putSegmentHashes(List<Pair<Integer, ByteArray>> segmentHashPairs) {}

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
