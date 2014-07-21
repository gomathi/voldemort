package voldemort.hashtrees;

public class SegmentData {

    private final String key;
    private final String digest;

    public SegmentData(final String key, final String digest) {
        this.key = key;
        this.digest = digest;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return digest;
    }

}
