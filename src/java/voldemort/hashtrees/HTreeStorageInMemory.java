package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HTreeStorageInMemory implements HTreeStorage {

    private final Map<Integer, String> segmentHashes = new TreeMap<Integer, String>();
    private final Map<Integer, TreeMap<String, String>> segDataBlocks = new HashMap<Integer, TreeMap<String, String>>();
    private final BitSet dirtySegments = new BitSet();

    @Override
    public void putSegmentHash(int segNodeId, String digest) {
        segmentHashes.put(segNodeId, digest);
    }

    @Override
    public void putSegmentData(int segId, String key, String digest) {
        if(!segDataBlocks.containsKey(segId))
            segDataBlocks.put(segId, new TreeMap<String, String>());
        segDataBlocks.get(segId).put(key, digest);
    }

    @Override
    public void deleteSegmentData(int segId, String key) {
        if(segDataBlocks.containsKey(segId)) {
            segDataBlocks.get(segId).remove(key);
        }
    }

    @Override
    public List<SegmentData> getSegmentBlock(int segId) {
        if(!segDataBlocks.containsKey(segId))
            return Collections.emptyList();
        TreeMap<String, String> segDataBlock = segDataBlocks.get(segId);
        List<SegmentData> result = new ArrayList<SegmentData>();
        for(Map.Entry<String, String> entry: segDataBlock.entrySet()) {
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
    public void setDirtySegmentBucket(int segId) {
        dirtySegments.set(segId);
    }

    @Override
    public List<Integer> getDirtySegmentBucketIds() {
        List<Integer> result = new ArrayList<Integer>();
        for(int itr = dirtySegments.nextSetBit(0); itr >= 0; itr = dirtySegments.nextSetBit(itr + 1)) {
            result.add(itr);
        }
        return result;
    }

    @Override
    public void unsetDirtySegmentBuckets(Collection<Integer> dirtySegIds) {
        for(int dirtySegId: dirtySegIds) {
            dirtySegments.clear(dirtySegId);
        }
    }

}
