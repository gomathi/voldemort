package voldemort.hashtrees;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import voldemort.hashtrees.storage.HashTreePersistentStorage;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.HashTreeStorageInMemory;
import voldemort.hashtrees.storage.Storage;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;

public class HashTreeImplTestUtils {

    public static final Random RANDOM = new Random(System.currentTimeMillis());
    public static final SegIdProviderTest segIdProvider = new SegIdProviderTest();
    public static final HashTreeIdProviderTest treeIdProvider = new HashTreeIdProviderTest();
    public static final int ROOT_NODE = 0;
    public static final int DEFAULT_TREE_ID = 1;
    public static final int DEFAULT_SEG_DATA_BLOCKS_COUNT = 1 << 10;
    public static final int DEFAULT_HTREE_SERVER_PORT_NO = 11111;

    public static class SegIdProviderTest implements SegmentIdProvider {

        @Override
        public int getSegmentId(ByteBuffer key) {
            return Integer.parseInt(ByteUtils.getString(key.array(), "UTF-8"));
        }

    }

    public static class HashTreeIdProviderTest implements HashTreeIdProvider {

        private final List<Integer> treeIds;

        public HashTreeIdProviderTest() {
            treeIds = new ArrayList<Integer>();
            treeIds.add(1);
        }

        @Override
        public int getTreeId(ByteBuffer key) {
            return 1;
        }

        @Override
        public List<Integer> getAllTreeIds() {
            return treeIds;
        }

    }

    public static class StorageImplTest implements Storage {

        final Map<ByteBuffer, ByteBuffer> localStorage = new HashMap<ByteBuffer, ByteBuffer>();
        volatile HashTree hashTree;

        public void setHashTree(final HashTree hashTree) {
            this.hashTree = hashTree;
        }

        @Override
        public ByteBuffer get(ByteBuffer key) {
            return localStorage.get(key);
        }

        @Override
        public void put(ByteBuffer key, ByteBuffer value) {
            localStorage.put(key, value);
            if(hashTree != null)
                hashTree.hPut(key, value);
        }

        @Override
        public ByteBuffer remove(ByteBuffer key) {
            ByteBuffer value = localStorage.remove(key);
            if(hashTree != null) {
                hashTree.hRemove(key);
                return value;
            }
            return null;
        }

        @Override
        public Iterator<Pair<ByteBuffer, ByteBuffer>> iterator() {
            return null;
        }

    }

    public static class HTreeComponents {

        public final HashTreeStorage hTStorage;
        public final StorageImplTest storage;
        public final HashTree hTree;

        public HTreeComponents(final HashTreeStorage hTStorage,
                               final StorageImplTest storage,
                               final HashTree hTree) {
            this.hTStorage = hTStorage;
            this.storage = storage;
            this.hTree = hTree;
        }
    }

    public static byte[] randomBytes() {
        byte[] emptyBuffer = new byte[8];
        RANDOM.nextBytes(emptyBuffer);
        return emptyBuffer;
    }

    public static ByteBuffer randomByteBuffer() {
        byte[] random = new byte[8];
        RANDOM.nextBytes(random);
        return ByteBuffer.wrap(random);
    }

    public static String randomDirName() {
        return "/tmp/test/random" + RANDOM.nextInt();
    }

    public static HTreeComponents createHashTree(int noOfSegDataBlocks,
                                                 final HashTreeIdProvider treeIdProv,
                                                 final SegmentIdProvider segIdPro,
                                                 final HashTreeStorage hTStorage) {
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegDataBlocks,
                                          treeIdProv,
                                          segIdPro,
                                          hTStorage,
                                          storage);
        hTree.updateHashTrees(true);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    public static HTreeComponents createHashTree(int noOfSegments, final HashTreeStorage hTStorage) {
        HashTreeIdProvider treeIdProvider = new HashTreeIdProviderTest();
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegments, treeIdProvider, hTStorage, storage);
        storage.setHashTree(hTree);
        hTree.updateHashTrees(true);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    public static HashTreeStorage generateInMemoryStore(int noOfSegDataBlocks) {
        return new HashTreeStorageInMemory(noOfSegDataBlocks);
    }

    private static HashTreeStorage generatePersistentStore(int noOfSegDataBlocks)
            throws IOException {
        return new HashTreePersistentStorage(randomDirName(), noOfSegDataBlocks);
    }

    public static HashTreeStorage[] generateInMemoryAndPersistentStores(int noOfSegDataBlocks)
            throws IOException {
        HashTreeStorage[] stores = new HashTreeStorage[2];
        stores[0] = generateInMemoryStore(noOfSegDataBlocks);
        stores[1] = generatePersistentStore(noOfSegDataBlocks);
        return stores;
    }

}
