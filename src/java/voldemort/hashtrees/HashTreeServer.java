package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

/**
 * Just wraps up {@link HashTree} and provides a view as
 * {@link HashTreeSyncInterface.Iface}. This is used by Thrift server.
 * 
 */
public class HashTreeServer implements HashTreeSyncInterface.Iface {

    private final HashTree hashTree;
    private final HashTreeManager hashTreeManager;

    public HashTreeServer(final HashTree hashTree, final HashTreeManager hashTreeManager) {
        this.hashTree = hashTree;
        this.hashTreeManager = hashTreeManager;
    }

    @Override
    public String ping() throws TException {
        return "ping";
    }

    @Override
    public void sPut(Map<ByteBuffer, ByteBuffer> keyValuePairs) throws TException {
        hashTree.sPut(keyValuePairs);
    }

    @Override
    public void sRemove(List<ByteBuffer> keys) throws TException {
        hashTree.sRemove(keys);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, List<Integer> nodeIds) throws TException {
        return hashTree.getSegmentHashes(treeId, nodeIds);
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) throws TException {
        return hashTree.getSegmentHash(treeId, nodeId);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) throws TException {
        return hashTree.getSegment(treeId, segId);
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) throws TException {
        return hashTree.getSegmentData(treeId, segId, key);
    }

    @Override
    public void deleteTreeNodes(int treeId, List<Integer> nodeIds) throws TException {
        hashTree.deleteTreeNodes(treeId, nodeIds);
    }

    @Override
    public void rebuildHashTree(long tokenNo, int treeId, long expFullRebuildTimeInt)
            throws TException {
        hashTreeManager.rebuild(tokenNo, treeId, expFullRebuildTimeInt);
    }

    @Override
    public void postRebuildHashTreeResponse(String hostName, long tokenNo, int treeId)
            throws TException {
        hashTreeManager.onRebuildHashTreeResponse(hostName, tokenNo, treeId);
    }

}
