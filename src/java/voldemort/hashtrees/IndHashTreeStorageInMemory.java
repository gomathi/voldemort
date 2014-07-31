package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import voldemort.utils.ByteArray;
import voldemort.utils.ThreadSafeBitSet;

/**
 * Hash tree can host multiple smaller hash trees. This stores the data for
 * smaller hash tree.
 * 
 */
class IndHashTreeStorageInMemory {

    private final ConcurrentMap<Integer, ByteArray> segmentHashes = new ConcurrentSkipListMap<Integer, ByteArray>();
    private final ConcurrentMap<Integer, ConcurrentSkipListMap<ByteArray, ByteArray>> segDataBlocks = new ConcurrentHashMap<Integer, ConcurrentSkipListMap<ByteArray, ByteArray>>();
    private final ThreadSafeBitSet dirtySegments;

    public IndHashTreeStorageInMemory(int noOfSegDataBlocks) {
        this.dirtySegments = new ThreadSafeBitSet(noOfSegDataBlocks);
    }

    public void putSegmentHash(int nodeId, ByteArray digest) {
        segmentHashes.put(nodeId, digest);
    }

    public void putSegmentData(int segId, ByteArray key, ByteArray digest) {
        segDataBlocks.putIfAbsent(segId, new ConcurrentSkipListMap<ByteArray, ByteArray>());
        segDataBlocks.get(segId).put(key, digest);
    }

    public void deleteSegmentData(int segId, ByteArray key) {
        Map<ByteArray, ByteArray> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock != null)
            segDataBlock.remove(key);
    }

    public List<SegmentData> getSegment(int segId) {
        ConcurrentMap<ByteArray, ByteArray> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock == null)
            return Collections.emptyList();
        List<SegmentData> result = new ArrayList<SegmentData>();
        for(Map.Entry<ByteArray, ByteArray> entry: segDataBlock.entrySet()) {
            result.add(new SegmentData(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds) {
        List<SegmentHash> result = new ArrayList<SegmentHash>();
        for(int nodeId: nodeIds) {
            ByteArray hash = segmentHashes.get(nodeId);
            if(hash != null)
                result.add(new SegmentHash(nodeId, hash));
        }
        return result;
    }

    public void setDirtySegment(int segId) {
        dirtySegments.set(segId);
    }

    public List<Integer> clearAndGetDirtySegments() {
        List<Integer> result = new ArrayList<Integer>();
        for(int itr = dirtySegments.clearAndGetNextSetBit(0); itr >= 0; itr = dirtySegments.clearAndGetNextSetBit(itr + 1)) {
            result.add(itr);
        }
        return result;
    }
}
