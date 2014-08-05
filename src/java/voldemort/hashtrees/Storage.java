package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.Iterator;

import voldemort.utils.Pair;

public interface Storage {

    ByteBuffer get(ByteBuffer key);

    void put(ByteBuffer key, ByteBuffer value);

    ByteBuffer remove(ByteBuffer key);

    Iterator<Pair<ByteBuffer, ByteBuffer>> iterator();
}
