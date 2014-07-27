package voldemort.hashtrees;

import voldemort.utils.ByteArray;

public interface Storage {

    ByteArray get(ByteArray key);

    void put(ByteArray key, ByteArray value);

    void remove(ByteArray key);
}
