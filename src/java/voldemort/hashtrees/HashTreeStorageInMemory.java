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
    private final ConcurrentMap<Integer, ThreadSafeBitSet> treeIdsAnddirtySegments;
    private final int noOfSegDataBlocks;

    public HashTreeStorageInMemory(int noOfSegDataBlocks) {
        this.noOfSegDataBlocks = noOfSegDataBlocks;
        this.treeIdsAnddirtySegments = new ConcurrentHashMap<Integer, ThreadSafeBitSet>();
    }

    @Override
    public void putSegmentHash(int treeId, int nodeId, ByteArray digest) {
        segmentHashes.put(nodeId, digest);
    }

    @Override
    public void putSegmentData(int treeId, int segId, ByteArray key, ByteArray digest) {
        segDataBlocks.putIfAbsent(segId, new ConcurrentSkipListMap<ByteArray, ByteArray>());
        segDataBlocks.get(segId).put(key, digest);
    }

    @Override
    public void deleteSegmentData(int treeId, int segId, ByteArray key) {
        Map<ByteArray, ByteArray> segDataBlock = segDataBlocks.get(segId);
        if(segDataBlock != null)
            segDataBlock.remove(key);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
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
    public List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds) {
        List<SegmentHash> result = new ArrayList<SegmentHash>();
        for(int nodeId: nodeIds) {
            ByteArray hash = segmentHashes.get(nodeId);
            if(hash != null)
                result.add(new SegmentHash(nodeId, hash));
        }
        return result;
    }

    @Override
    public void setDirtySegment(int treeId, int segId) {
        treeIdsAnddirtySegments.putIfAbsent(treeId, new ThreadSafeBitSet(noOfSegDataBlocks));
        treeIdsAnddirtySegments.get(treeId).set(segId);
    }

    @Override
    public List<Integer> clearAndGetDirtySegments(int treeId) {
        treeIdsAnddirtySegments.putIfAbsent(treeId, new ThreadSafeBitSet(noOfSegDataBlocks));
        List<Integer> result = new ArrayList<Integer>();
        for(int itr = treeIdsAnddirtySegments.get(treeId).clearAndGetNextSetBit(0); itr >= 0; itr = treeIdsAnddirtySegments.get(treeId)
                                                                                                                           .clearAndGetNextSetBit(itr + 1)) {
            result.add(itr);
        }
        return result;
    }

}
