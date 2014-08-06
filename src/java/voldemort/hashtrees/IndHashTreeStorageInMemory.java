package voldemort.hashtrees;

import java.nio.ByteBuffer;
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
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.utils.AtomicBitSet;

/**
 * Hash tree can host multiple similar hash trees. This stores the data for one
 * such hash tree.
 * 
 */
@Threadsafe
class IndHashTreeStorageInMemory {

    private final ConcurrentMap<Integer, ByteBuffer> segmentHashes = new ConcurrentSkipListMap<Integer, ByteBuffer>();
    private final ConcurrentMap<Integer, ConcurrentSkipListMap<ByteBuffer, ByteBuffer>> segDataBlocks = new ConcurrentHashMap<Integer, ConcurrentSkipListMap<ByteBuffer, ByteBuffer>>();
    private final AtomicBitSet dirtySegments;
    private final AtomicLong rebuiltTreeTs = new AtomicLong(0);

    public IndHashTreeStorageInMemory(int noOfSegDataBlocks) {
        this.dirtySegments = new AtomicBitSet(noOfSegDataBlocks);
    }

    public void putSegmentHash(int nodeId, ByteBuffer digest) {
        segmentHashes.put(nodeId, digest);
    }

    public void putSegmentData(int segId, ByteBuffer key, ByteBuffer digest) {
        if(!segDataBlocks.containsKey(segId))
            segDataBlocks.putIfAbsent(segId, new ConcurrentSkipListMap<ByteBuffer, ByteBuffer>());
        segDataBlocks.get(segId).put(key, digest);
    }

    public SegmentData getSegmentData(int segId, ByteBuffer key) {
        ConcurrentSkipListMap<ByteBuffer, ByteBuffer> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock != null) {
            ByteBuffer value = segDataBlock.get(key);
            if(value != null)
                return new SegmentData(key, value);
        }
        return null;
    }

    public void deleteSegmentData(int segId, ByteBuffer key) {
        Map<ByteBuffer, ByteBuffer> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock != null)
            segDataBlock.remove(key);
    }

    public List<SegmentData> getSegment(int segId) {
        ConcurrentMap<ByteBuffer, ByteBuffer> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock == null)
            return Collections.emptyList();
        List<SegmentData> result = new ArrayList<SegmentData>();
        for(Map.Entry<ByteBuffer, ByteBuffer> entry: segDataBlock.entrySet()) {
            result.add(new SegmentData(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds) {
        List<SegmentHash> result = new ArrayList<SegmentHash>();
        for(int nodeId: nodeIds) {
            ByteBuffer hash = segmentHashes.get(nodeId);
            if(hash != null)
                result.add(new SegmentHash(nodeId, hash));
        }
        return result;
    }

    public SegmentHash getSegmentHash(int nodeId) {
        ByteBuffer hash = segmentHashes.get(nodeId);
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

    public void setLastTreeBuildTimestamp(long timestamp) {
        long oldValue = rebuiltTreeTs.get();
        while(oldValue < timestamp) {
            if(rebuiltTreeTs.compareAndSet(oldValue, timestamp))
                break;
            oldValue = rebuiltTreeTs.get();
        }
    }

    public long getLastTreeBuildTimestamp() {
        long value = rebuiltTreeTs.get();
        if(value != 0)
            return value;
        return 1;
    }
}
