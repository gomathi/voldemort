package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.hashtrees.HashTreeImpl.SegmentIdProvider;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;

public class HashTreeImplTest {

    private static class SegIdProviderTest implements SegmentIdProvider {

        @Override
        public int getSegmentId(ByteArray key) {
            return Integer.parseInt(ByteUtils.getString(key.get(), "UTF-8"));
        }

    }

    private static class HashTreeIdProviderTest implements HashTreeIdProvider {

        private final List<Integer> treeIds;

        public HashTreeIdProviderTest() {
            treeIds = new ArrayList<Integer>();
            treeIds.add(1);
        }

        @Override
        public int getTreeId(ByteArray key) {
            return 1;
        }

        @Override
        public List<Integer> getAllTreeIds() {
            return treeIds;
        }

    }

    private static class StorageImplTest implements Storage {

        private final ConcurrentMap<ByteArray, ByteArray> localStorage = new ConcurrentHashMap<ByteArray, ByteArray>();

        @Override
        public ByteArray get(ByteArray key) {
            return localStorage.get(key);
        }

        @Override
        public void put(ByteArray key, ByteArray value) {
            localStorage.put(key, value);
        }

        @Override
        public void remove(ByteArray key) {
            localStorage.remove(key);
        }

    }

    private static final SegIdProviderTest segIdProvider = new SegIdProviderTest();
    private static final HashTreeIdProviderTest treeIdProvider = new HashTreeIdProviderTest();
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    private static byte[] randomBytes() {
        byte[] emptyBuffer = new byte[8];
        RANDOM.nextBytes(emptyBuffer);
        return emptyBuffer;
    }

    private static Pair<HashTree, HashTreeStorage> getHashTreeImpl(int noOfSegDataBlocks,
                                                                   final StorageImplTest localStorage) {
        HashTreeStorage localTreeStorage = new HashTreeStorageInMemory(noOfSegDataBlocks);
        return new Pair<HashTree, HashTreeStorage>(new HashTreeImpl(treeIdProvider,
                                                                    segIdProvider,
                                                                    localTreeStorage,
                                                                    localStorage), localTreeStorage);
    }

    @Test
    public void testPut() {

        int noOfSegDataBlocks = 1024;
        StorageImplTest testStorage = new StorageImplTest();
        Pair<HashTree, HashTreeStorage> pair = getHashTreeImpl(noOfSegDataBlocks, testStorage);
        HashTree testTree = pair.getFirst();
        HashTreeStorage testTreeStorage = pair.getSecond();

        ByteArray key = new ByteArray("1".getBytes());
        ByteArray value = new ByteArray(randomBytes());
        testTree.put(key, value);
        ByteArray digest = new ByteArray(ByteUtils.sha1(value.get()));

        SegmentData segData = testTreeStorage.getSegmentData(1, 1, key);
        Assert.assertNotNull(segData);
        Assert.assertEquals(key, segData.getKey());
        Assert.assertEquals(digest, segData.getValue());

        List<Integer> dirtySegs = testTreeStorage.clearAndGetDirtySegments(1);
        Assert.assertEquals(1, dirtySegs.size());
        Assert.assertEquals(1, dirtySegs.get(0).intValue());
    }

    @Test
    public void testRemove() {

        int noOfSegDataBlocks = 1024;
        StorageImplTest testStorage = new StorageImplTest();
        Pair<HashTree, HashTreeStorage> pair = getHashTreeImpl(noOfSegDataBlocks, testStorage);
        HashTree testTree = pair.getFirst();
        HashTreeStorage testTreeStorage = pair.getSecond();

        ByteArray key = new ByteArray("2".getBytes());
        ByteArray value = new ByteArray(randomBytes());
        testTree.put(key, value);
        testTree.remove(key);

        SegmentData segData = testTreeStorage.getSegmentData(1, 2, key);
        Assert.assertNull(segData);

        List<Integer> dirtySegs = testTreeStorage.clearAndGetDirtySegments(1);
        Assert.assertEquals(1, dirtySegs.size());
        Assert.assertEquals(2, dirtySegs.get(0).intValue());
    }

    @Test
    public void testUpdateTree() {
        int noOfSegDataBlocks = 1024;
    }
}
