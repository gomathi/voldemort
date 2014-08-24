package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

/**
 * A {@link HashTree} implementation that wraps up
 * {@link HashTreeSyncInterface.Iface} client and forwards the calls to the
 * remote tree.
 * 
 */
public class HashTreeClient implements HashTree {

    private final HashTreeSyncInterface.Iface remoteTree;

    public HashTreeClient(final HashTreeSyncInterface.Iface remoteTree) {
        this.remoteTree = remoteTree;
    }

    @Override
    public void sPut(Map<ByteBuffer, ByteBuffer> keyValuePairs) throws TException {
        remoteTree.sPut(keyValuePairs);
    }

    @Override
    public void sRemove(List<ByteBuffer> keys) throws TException {
        remoteTree.sRemove(keys);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, List<Integer> nodeIds) throws TException {
        return remoteTree.getSegmentHashes(treeId, nodeIds);
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) throws TException {
        return remoteTree.getSegmentHash(treeId, nodeId);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) throws TException {
        return remoteTree.getSegment(treeId, segId);
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) throws TException {
        return remoteTree.getSegmentData(treeId, segId, key);
    }

    @Override
    public void deleteTreeNodes(int treeId, List<Integer> nodeIds) throws TException {
        remoteTree.deleteTreeNodes(treeId, nodeIds);
    }

    @Override
    public void hPut(ByteBuffer key, ByteBuffer value) {
        throw new UnsupportedOperationException("Remote tree does not support this operation.");
    }

    @Override
    public void hRemove(ByteBuffer key) {
        throw new UnsupportedOperationException("Remote tree does not support this operation.");
    }

    @Override
    public boolean synch(int treeId, HashTree remoteTree) throws TException {
        throw new UnsupportedOperationException("Remote tree does not support this operation.");
    }

    @Override
    public void rebuildHashTrees(boolean fullRebuild) {
        throw new UnsupportedOperationException("Remote tree does not support this operation.");
    }

    @Override
    public void rebuildHashTree(int treeId, boolean fullRebuild) {
        throw new UnsupportedOperationException("Remote tree does not support this operation.");
    }

    @Override
    public long getLastFullyRebuiltTimeStamp(int treeId) {
        throw new UnsupportedOperationException("Remote tree does not support this operation.");
    }

}
