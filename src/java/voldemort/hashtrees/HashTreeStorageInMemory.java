package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

/**
 * In memory implementation of {@link HashTreeStorage}, can be used for testing,
 * or in nodes where the partition itself is small enough.
 * 
 */
@Threadsafe
public class HashTreeStorageInMemory implements HashTreeStorage {

    private final int noOfSegDataBlocks;
    private final ConcurrentMap<Integer, IndHashTreeStorageInMemory> treeIdAndIndHashTree = new ConcurrentHashMap<Integer, IndHashTreeStorageInMemory>();

    public HashTreeStorageInMemory(int noOfSegDataBlocks) {
        this.noOfSegDataBlocks = noOfSegDataBlocks;
    }

    private IndHashTreeStorageInMemory getIndHTree(int treeId) {
        if(!treeIdAndIndHashTree.containsKey(treeId))
            treeIdAndIndHashTree.putIfAbsent(treeId,
                                             new IndHashTreeStorageInMemory(noOfSegDataBlocks));
        return treeIdAndIndHashTree.get(treeId);
    }

    @Override
    public void putSegmentData(int treeId, int segId, ByteBuffer key, ByteBuffer digest) {
        getIndHTree(treeId).putSegmentData(segId, key, digest);
    }

    @Override
    public void deleteSegmentData(int treeId, int segId, ByteBuffer key) {
        getIndHTree(treeId).deleteSegmentData(segId, key);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return getIndHTree(treeId).getSegment(segId);
    }

    @Override
    public void putSegmentHash(int treeId, int nodeId, ByteBuffer digest) {
        getIndHTree(treeId).putSegmentHash(nodeId, digest);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds) {
        return getIndHTree(treeId).getSegmentHashes(nodeIds);
    }

    @Override
    public void setDirtySegment(int treeId, int segId) {
        getIndHTree(treeId).setDirtySegment(segId);
    }

    @Override
    public List<Integer> clearAndGetDirtySegments(int treeId) {
        return getIndHTree(treeId).clearAndGetDirtySegments();
    }

    @Override
    public void deleteTree(int treeId) {
        treeIdAndIndHashTree.remove(treeId);
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) {
        return getIndHTree(treeId).getSegmentData(segId, key);
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) {
        return getIndHTree(treeId).getSegmentHash(nodeId);
    }

    @Override
    public void clearAllSegments(int treeId) {
        getIndHTree(treeId).clearDirtySegments();
    }

    @Override
    public void setLastFullyTreeBuiltTimestamp(int treeId, long timestamp) {
        getIndHTree(treeId).setLastFullyRebuiltTimestamp(timestamp);
    }

    @Override
    public long getLastFullyTreeReBuiltTimestamp(int treeId) {
        return getIndHTree(treeId).getLastTreeFullyRebuiltTimestamp();
    }

    @Override
    public void setLastHashTreeUpdatedTimestamp(int treeId, long timestamp) {
        getIndHTree(treeId).setLastHashTreeUpdatedTimestamp(timestamp);
    }

    @Override
    public long getLastHashTreeUpdatedTimestamp(int treeId) {
        return getIndHTree(treeId).getLastHashTreeUpdatedTimestamp();
    }

}