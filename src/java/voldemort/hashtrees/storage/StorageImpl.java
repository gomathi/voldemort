package voldemort.hashtrees.storage;

import java.nio.ByteBuffer;
import java.util.Iterator;

import voldemort.utils.Pair;

public class StorageImpl implements Storage {

    @Override
    public ByteBuffer get(ByteBuffer key) {
        return null;
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {}

    @Override
    public ByteBuffer remove(ByteBuffer key) {
        return null;
    }

    @Override
    public Iterator<Pair<ByteBuffer, ByteBuffer>> iterator() {
        return null;
    }

}
