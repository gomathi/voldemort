package voldemort.hashtrees;

import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

import com.google.common.primitives.Bytes;

/**
 * Usually stored on disk.
 * 
 */
public class SegmentData {

    private final ByteArray key;
    private final ByteArray digest;

    public SegmentData(final ByteArray key, final ByteArray digest) {
        this.key = key;
        this.digest = digest;
    }

    public ByteArray getKey() {
        return key;
    }

    public ByteArray getValue() {
        return digest;
    }

    public String getKeyAndDigestString() {
        return ByteUtils.toHexString(Bytes.concat(key.get(), digest.get()));
    }

}
