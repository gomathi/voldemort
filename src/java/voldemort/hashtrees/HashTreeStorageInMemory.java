package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.ByteArray;

@Threadsafe
public class HashTreeStorageInMemory implements HashTreeStorage {

    private final ConcurrentMap<Integer, ByteArray> segmentHashes = new ConcurrentSkipListMap<Integer, ByteArray>();
    private final ConcurrentMap<Integer, ConcurrentSkipListMap<ByteArray, ByteArray>> segDataBlocks = new ConcurrentHashMap<Integer, ConcurrentSkipListMap<ByteArray, ByteArray>>();
    private final ThreadSafeBitSet dirtySegments;

    public HashTreeStorageInMemory(int noOfSegDataBlocks) {
        dirtySegments = new ThreadSafeBitSet(noOfSegDataBlocks);
    }

    @Override
    public void putSegmentHash(int nodeId, ByteArray digest) {
        segmentHashes.put(nodeId, digest);
    }

    @Override
    public void putSegmentData(int segId, ByteArray key, ByteArray digest) {
        segDataBlocks.putIfAbsent(segId, new ConcurrentSkipListMap<ByteArray, ByteArray>());
        segDataBlocks.get(segId).put(key, digest);
    }

    @Override
    public void deleteSegmentData(int segId, ByteArray key) {
        Map<ByteArray, ByteArray> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock != null)
            segDataBlock.remove(key);
    }

    @Override
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

    @Override
    public List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds) {
        List<SegmentHash> result = new ArrayList<SegmentHash>();
        for(int nodeId: nodeIds) {
            ByteArray hash = segmentHashes.get(nodeId);
            if(hash != null)
                result.add(new SegmentHash(nodeId, hash));
        }
        return result;
    }

    @Override
    public void setDirtySegment(int segId) {
        dirtySegments.set(segId);
    }

    @Override
    public List<Integer> getAndClearDirtySegments() {
        List<Integer> result = new ArrayList<Integer>();
        for(int itr = dirtySegments.clearAndGetNextSetBit(0); itr >= 0; itr = dirtySegments.clearAndGetNextSetBit(itr + 1)) {
            result.add(itr);
        }
        return result;
    }

}
