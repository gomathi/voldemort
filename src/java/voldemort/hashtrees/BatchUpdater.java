package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

@NotThreadsafe
public class BatchUpdater {

    private final int batchSize;
    private final HashTree remoteHTree;
    private final Map<ByteArray, ByteArray> keyValuePairsToAdd = new HashMap<ByteArray, ByteArray>();
    private final List<ByteArray> keysToBeRemoved = new ArrayList<ByteArray>();

    public BatchUpdater(final int batchSize, final HashTree remoteHTree) {
        this.batchSize = batchSize;
        this.remoteHTree = remoteHTree;
    }

    public void addKeys(List<ByteArray> keys) {
        Map<ByteArray, ByteArray> kvPairs = getKeysAndValues(keys);
        keyValuePairsToAdd.putAll(kvPairs);
    }

    public void addKey(ByteArray key) {
        Pair<ByteArray, ByteArray> keyValue = getKeyValue(key);
        keyValuePairsToAdd.put(keyValue.getFirst(), keyValue.getSecond());
    }

    private Pair<ByteArray, ByteArray> getKeyValue(ByteArray key) {
        return null;
    }

    private Map<ByteArray, ByteArray> getKeysAndValues(List<ByteArray> keys) {
        return null;
    }

    public void removeKeys(List<ByteArray> keys) {
        keysToBeRemoved.addAll(keys);
    }

    public void removeKey(ByteArray key) {
        keysToBeRemoved.add(key);
    }

    public void flush() {

    }
}
