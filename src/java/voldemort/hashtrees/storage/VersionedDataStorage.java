package voldemort.hashtrees.storage;

import java.nio.ByteBuffer;
import java.util.Iterator;

import voldemort.utils.Pair;

/**
 * Stores (key,value) with a monotonically increasing number. Using this number
 * clients can request for all the changes happened since this number.
 * 
 */
public interface VersionedDataStorage {

    void putVersionedData(int treeId, ByteBuffer key, ByteBuffer value);

    Iterator<Pair<ByteBuffer, ByteBuffer>> getVersionedData(int treeId);

    Iterator<Pair<ByteBuffer, ByteBuffer>> getVersionedData(int treeId, long versionNo);
}
