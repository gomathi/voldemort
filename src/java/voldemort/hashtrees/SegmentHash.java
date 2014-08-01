package voldemort.hashtrees;

import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

/**
 * Usually stored in memory.
 * 
 */
public class SegmentHash {

    private final ByteArray hash;
    private final int nodeId;

    public SegmentHash(final int nodeId, final ByteArray hash) {
        this.hash = hash;
        this.nodeId = nodeId;
    }

    public ByteArray getHash() {
        return hash;
    }

    public String getHashString() {
        return ByteUtils.toHexString(hash.get());
    }

    public int getNodeId() {
        return nodeId;
    }

}
