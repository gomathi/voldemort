package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.AtomicBitSet;
import voldemort.utils.ByteArray;

/**
 * Hash tree can host multiple similar hash trees. This stores the data for one
 * such hash tree.
 * 
 */
@Threadsafe
class IndHashTreeStorageInMemory {

    private final ConcurrentMap<Integer, ByteArray> segmentHashes = new ConcurrentSkipListMap<Integer, ByteArray>();
    private final ConcurrentMap<Integer, ConcurrentSkipListMap<ByteArray, ByteArray>> segDataBlocks = new ConcurrentHashMap<Integer, ConcurrentSkipListMap<ByteArray, ByteArray>>();
    private final AtomicBitSet dirtySegments;
    private final AtomicLong rebuiltTreeTs = new AtomicLong(0);

    public IndHashTreeStorageInMemory(int noOfSegDataBlocks) {
        this.dirtySegments = new AtomicBitSet(noOfSegDataBlocks);
    }

    public void putSegmentHash(int nodeId, ByteArray digest) {
        segmentHashes.put(nodeId, digest);
    }

    public void putSegmentData(int segId, ByteArray key, ByteArray digest) {
        segDataBlocks.putIfAbsent(segId, new ConcurrentSkipListMap<ByteArray, ByteArray>());
        segDataBlocks.get(segId).put(key, digest);
    }

    public SegmentData getSegmentData(int segId, ByteArray key) {
        ConcurrentSkipListMap<ByteArray, ByteArray> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock != null) {
            ByteArray value = segDataBlock.get(key);
            if(value != null)
                return new SegmentData(key, value);
        }
        return null;
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

    public SegmentHash getSegmentHash(int nodeId) {
        ByteArray hash = segmentHashes.get(nodeId);
        if(hash == null)
            return null;
        return new SegmentHash(nodeId, hash);
    }

    public void setDirtySegment(int segId) {
        dirtySegments.set(segId);
    }

    public void clearDirtySegments() {
        dirtySegments.clear();
    }

    public List<Integer> clearAndGetDirtySegments() {
        return dirtySegments.clearAndGetAllSetBits();
    }

    public boolean setLastTreeBuildTimestamp(long timestamp) {
        long oldValue = rebuiltTreeTs.get();
        while(oldValue < timestamp) {
            if(rebuiltTreeTs.compareAndSet(oldValue, timestamp))
                return true;
            oldValue = rebuiltTreeTs.get();
        }
        return false;
    }

    public long getLastTreeBuildTimestamp() {
        return rebuiltTreeTs.get();
    }
}
