package voldemort.hashtrees;

import static voldemort.hashtrees.HashTreeImplTestUtils.treeIdProvider;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.thrift.TException;

import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.Storage;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface.Iface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

public class HashTreeImplTestObj extends HashTreeImpl {

    private final BlockingQueue<HashTreeImplEvent> events;

    public HashTreeImplTestObj(final int noOfSegments,
                               final HashTreeStorage htStorage,
                               final Storage storage,
                               BlockingQueue<HashTreeImplEvent> events) {
        super(noOfSegments, treeIdProvider, htStorage, storage);
        this.events = events;
    }

    @Override
    public String ping() throws TException {
        return super.ping();
    }

    @Override
    public void sPut(Map<ByteBuffer, ByteBuffer> keyValuePairs) {
        super.sPut(keyValuePairs);
    }

    @Override
    public void sRemove(List<ByteBuffer> keys) {
        super.sRemove(keys);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, List<Integer> nodeIds) {
        return super.getSegmentHashes(treeId, nodeIds);
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) {
        return super.getSegmentHash(treeId, nodeId);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return super.getSegment(treeId, segId);
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) {
        return super.getSegmentData(treeId, segId, key);
    }

    @Override
    public boolean isReadyForSynch(int treeId) {
        boolean result = super.isReadyForSynch(treeId);
        events.add(HashTreeImplEvent.SYNCH_INITIATED);
        return result;
    }

    @Override
    public void deleteTreeNodes(int treeId, List<Integer> nodeIds) {
        super.deleteTreeNodes(treeId, nodeIds);
    }

    @Override
    public void hPut(ByteBuffer key, ByteBuffer value) {
        super.hPut(key, value);
    }

    @Override
    public void hRemove(ByteBuffer key) {
        super.hRemove(key);
    }

    @Override
    public boolean synch(int treeId, Iface remoteTree) throws TException {
        boolean result = super.synch(treeId, remoteTree);
        events.add(HashTreeImplEvent.SYNCH);
        return result;
    }

    @Override
    public void addTreeToSyncList(String hostName, int treeId) {
        super.addTreeToSyncList(hostName, treeId);
    }

    @Override
    public void removeTreeFromSyncList(String hostName, int treeId) {
        super.removeTreeFromSyncList(hostName, treeId);
    }

    @Override
    public void updateHashTrees(boolean fullRebuild) {
        super.updateHashTrees(fullRebuild);
    }

    @Override
    public void updateHashTree(int treeId, boolean fullRebuild) {
        super.updateHashTree(treeId, fullRebuild);
        if(!fullRebuild)
            events.add(HashTreeImplEvent.UPDATE_SEGMENT);
        else
            events.add(HashTreeImplEvent.UPDATE_FULL_TREE);
    }

}