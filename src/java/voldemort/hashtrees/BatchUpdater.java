package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.utils.ByteArray;

public class BatchUpdater {

    private final int batchSize;
    private final HashTree remoteHTree;
    private final Map<ByteArray, ByteArray> keyValuePairsToAdd = new HashMap<ByteArray, ByteArray>();
    private final List<ByteArray> keysToBeRemoved = new ArrayList<ByteArray>();

    public BatchUpdater(final int batchSize, final HashTree remoteHTree) {
        this.batchSize = batchSize;
        this.remoteHTree = remoteHTree;
    }

    public void addKeys(List<ByteArray> input) {
        Map<ByteArray, ByteArray> kvPairs = getValues(input);
        keyValuePairsToAdd.putAll(kvPairs);
    }

    private Map<ByteArray, ByteArray> getValues(List<ByteArray> input) {
        return null;
    }

    public void removeKeys(List<ByteArray> input) {
        keysToBeRemoved.addAll(input);
    }

    public void close() {

    }
}
