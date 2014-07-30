package voldemort.hashtrees;

import java.util.List;

import voldemort.utils.ByteArray;

public class HashTreeIdProviderImpl implements HashTreeIdProvider {

    @Override
    public int getTreeId(ByteArray key) {
        return 0;
    }

    @Override
    public List<Integer> getAllTreeIds() {
        return null;
    }

}
