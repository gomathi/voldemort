package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchUpdater {

    private final int batchSize;
    private final HTree remoteHTree;
    private final Map<String, String> keyValuePairsToAdd = new HashMap<String, String>();
    private final List<String> keysToBeRemoved = new ArrayList<String>();

    public BatchUpdater(final int batchSize, final HTree remoteHTree) {
        this.batchSize = batchSize;
        this.remoteHTree = remoteHTree;
    }

    public void addKeys(List<String> input) {
        Map<String, String> kvPairs = getValues(input);
        keyValuePairsToAdd.putAll(kvPairs);
    }

    private Map<String, String> getValues(List<String> input) {
        return null;
    }

    public void removeKeys(List<String> input) {
        keysToBeRemoved.addAll(input);
    }

    public void close() {

    }
}
