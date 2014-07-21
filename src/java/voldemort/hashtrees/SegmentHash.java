package voldemort.hashtrees;

public class SegmentHash {

    private final String hash;
    private final int nodeId;

    public SegmentHash(final int nodeId, final String hash) {
        this.hash = hash;
        this.nodeId = nodeId;
    }

    public String getHash() {
        return hash;
    }

    public long getNodeId() {
        return nodeId;
    }

}
