package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.utils.ByteArray;

/**
 * A class which forwards the calls to a remote node which is running another
 * hash tree.
 * 
 */
public class RemoteHashTree implements HashTree {

    @Override
    public void hPut(ByteArray key, ByteArray value) {}

    @Override
    public void hRemove(ByteArray key) {}

    @Override
    public void sPut(ByteArray key, ByteArray value) {}

    @Override
    public void sPut(Map<ByteArray, ByteArray> keyValuePairs) {}

    @Override
    public void sRemove(ByteArray key) {}

    @Override
    public void sRemove(List<ByteArray> key) {}

    @Override
    public boolean synch(int treeId, HashTree remoteTree) {
        return false;
    }

    @Override
    public void addTreeToSyncList(String hostName, HashTree remoteTree) {}

    @Override
    public void removeTreeFromSyncList(String hostName) {}

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds) {
        return null;
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return null;
    }

    @Override
    public void updateHashTrees(boolean rebuild) {}

    @Override
    public void deleteTreeNodes(int treeId, Collection<Integer> nodeIds) {}

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) {
        return null;
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteArray key) {
        return null;
    }

    @Override
    public void updateHashTree(int treeId, boolean fullRebuild) {}

}
