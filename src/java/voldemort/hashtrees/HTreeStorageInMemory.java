package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

public class HTreeStorageInMemory implements HTreeStorage {

    private final Map<Integer, ByteArray> segmentHashes = new TreeMap<Integer, ByteArray>();
    private final Map<Integer, TreeMap<ByteArray, ByteArray>> segDataBlocks = new HashMap<Integer, TreeMap<ByteArray, ByteArray>>();
    private final BitSet dirtySegments = new BitSet();

    @Override
    public void putSegmentHash(int segNodeId, ByteArray digest) {
        segmentHashes.put(segNodeId, digest);
    }

    @Override
    public void putSegmentData(int segId, ByteArray key, ByteArray digest) {
        if(!segDataBlocks.containsKey(segId))
            segDataBlocks.put(segId, new TreeMap<ByteArray, ByteArray>());
        segDataBlocks.get(segId).put(key, digest);
    }

    @Override
    public void deleteSegmentData(int segId, ByteArray key) {
        if(segDataBlocks.containsKey(segId) && segDataBlocks.get(segId).containsKey(key)) {
            segDataBlocks.get(segId).remove(key);
        }
    }

    @Override
    public List<SegmentData> getSegment(int segId) {
        if(!segDataBlocks.containsKey(segId))
            return Collections.emptyList();
        TreeMap<ByteArray, ByteArray> segDataBlock = segDataBlocks.get(segId);
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
            if(segmentHashes.containsKey(nodeId))
                result.add(new SegmentHash(nodeId, segmentHashes.get(nodeId)));
        }
        return result;
    }

    @Override
    public void setDirtySegment(int segId) {
        dirtySegments.set(segId);
    }

    @Override
    public List<Integer> getDirtySegments() {
        List<Integer> result = new ArrayList<Integer>();
        for(int itr = dirtySegments.nextSetBit(0); itr >= 0; itr = dirtySegments.nextSetBit(itr + 1)) {
            result.add(itr);
        }
        return result;
    }

    @Override
    public void unsetDirtySegmens(Collection<Integer> dirtySegIds) {
        for(int dirtySegId: dirtySegIds) {
            dirtySegments.clear(dirtySegId);
        }
    }

    @Override
    public void deleteSegments(Collection<Integer> segIds) {
        for(int segId: segIds)
            deleteSegment(segId);
    }

    @Override
    public void deleteSegment(int segId) {
        if(segDataBlocks.containsKey(segId)) {
            segDataBlocks.remove(segId);
        }
    }

    @Override
    public void putSegmentHashes(List<Pair<Integer, ByteArray>> segmentHashPairs) {

    }

}
