package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A class which forwards the calls to a remote node which is running another
 * hash tree.
 * 
 */
public class RemoteHTreeImpl implements HTree {

    public RemoteHTreeImpl() {

    }

    @Override
    public void put(String key, String value) {}

    @Override
    public void remove(String key) {}

    @Override
    public void batchSPut(Map<String, String> keyValuePairs) {}

    @Override
    public void batchSRemove(List<String> key) {}

    @Override
    public void update(HTree remoteTree) {}

    @Override
    public List<SegmentHash> getSegmentHashes(Collection<Integer> nodeIds) {
        return null;
    }

    @Override
    public List<SegmentData> getSegment(int segId) {
        return null;
    }

    @Override
    public void rebuildHTree() {}

    @Override
    public void deleteNodes(Collection<Integer> nodeIds) {}

}
