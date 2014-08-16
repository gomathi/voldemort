package voldemort.hashtrees;

import java.nio.ByteBuffer;

public class DefaultSegIdProviderImpl implements SegmentIdProvider {

    private final int noOfBuckets;

    public DefaultSegIdProviderImpl(int noOfBuckets) {
        this.noOfBuckets = noOfBuckets;
    }

    @Override
    public int getSegmentId(ByteBuffer key) {
        int hcode = key.hashCode();
        return hcode & (noOfBuckets - 1);
    }

}