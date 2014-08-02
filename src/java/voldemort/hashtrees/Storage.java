package voldemort.hashtrees;

import java.util.Iterator;

import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

public interface Storage {

    ByteArray get(ByteArray key);

    void put(ByteArray key, ByteArray value);

    ByteArray remove(ByteArray key);

    Iterator<Pair<ByteArray, ByteArray>> iterator();
}
