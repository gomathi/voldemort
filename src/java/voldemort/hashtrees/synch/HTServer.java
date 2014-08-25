package voldemort.hashtrees.synch;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import voldemort.hashtrees.core.HashTree;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

/**
 * Just wraps up {@link HashTree} and provides a view as
 * {@link HashTreeSyncInterface.Iface}. This is used by Thrift server.
 * 
 */
public class HTServer implements HashTreeSyncInterface.Iface {

    private final HashTree hashTree;
    private final HTSyncManagerImpl hashTreeManager;

    public HTServer(final HashTree hashTree, final HTSyncManagerImpl hashTreeManager) {
        this.hashTree = hashTree;
        this.hashTreeManager = hashTreeManager;
    }

    @Override
    public String ping() throws TException {
        return "ping";
    }

    @Override
    public void sPut(Map<ByteBuffer, ByteBuffer> keyValuePairs) throws TException {
        try {
            hashTree.sPut(keyValuePairs);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void sRemove(List<ByteBuffer> keys) throws TException {
        try {
            hashTree.sRemove(keys);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, List<Integer> nodeIds) throws TException {
        try {
            return hashTree.getSegmentHashes(treeId, nodeIds);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) throws TException {
        try {
            return hashTree.getSegmentHash(treeId, nodeId);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) throws TException {
        try {
            return hashTree.getSegment(treeId, segId);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) throws TException {
        try {
            return hashTree.getSegmentData(treeId, segId, key);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void deleteTreeNodes(int treeId, List<Integer> nodeIds) throws TException {
        try {
            hashTree.deleteTreeNodes(treeId, nodeIds);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void rebuildHashTree(long tokenNo, int treeId, long expFullRebuildTimeInt)
            throws TException {
        try {
            hashTreeManager.onRebuildHashTreeRequest(tokenNo, treeId, expFullRebuildTimeInt);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

    @Override
    public void postRebuildHashTreeResponse(String hostName, long tokenNo, int treeId)
            throws TException {
        try {
            hashTreeManager.onRebuildHashTreeResponse(hostName, tokenNo, treeId);
        } catch(Exception e) {
            throw new TException(e);
        }
    }

}
