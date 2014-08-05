package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.List;

public class HashTreeIdProviderImpl implements HashTreeIdProvider {

    @Override
    public int getTreeId(ByteBuffer key) {
        return 0;
    }

    @Override
    public List<Integer> getAllTreeIds() {
        return null;
    }

}
