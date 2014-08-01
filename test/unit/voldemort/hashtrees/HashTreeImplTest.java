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

import com.google.common.primitives.Bytes;

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
        private volatile HashTree hashTree;

        public void setHashTree(final HashTree hashTree) {
            this.hashTree = hashTree;
        }

        @Override
        public ByteArray get(ByteArray key) {
            return localStorage.get(key);
        }

        @Override
        public void put(ByteArray key, ByteArray value) {
            localStorage.put(key, value);
            if(hashTree != null)
                hashTree.hPut(key, value);
        }

        @Override
        public void remove(ByteArray key) {
            localStorage.remove(key);
            if(hashTree != null)
                hashTree.hRemove(key);
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
        return new Pair<HashTree, HashTreeStorage>(new HashTreeImpl(noOfSegDataBlocks,
                                                                    treeIdProvider,
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
        testTree.hPut(key, value);
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
        testTree.hPut(key, value);
        testTree.hRemove(key);

        SegmentData segData = testTreeStorage.getSegmentData(1, 2, key);
        Assert.assertNull(segData);

        List<Integer> dirtySegs = testTreeStorage.clearAndGetDirtySegments(1);
        Assert.assertEquals(1, dirtySegs.size());
        Assert.assertEquals(2, dirtySegs.get(0).intValue());
    }

    private static class HTreeComponents {

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

    private static HTreeComponents createHashTreeAndStorage(int noOfSegments,
                                                            int noOfChildrenPerParent) {
        HashTreeIdProvider treeIdProvider = new HashTreeIdProviderTest();
        HashTreeStorage hTStorage = new HashTreeStorageInMemory(noOfSegments);
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegments,
                                          noOfChildrenPerParent,
                                          treeIdProvider,
                                          hTStorage,
                                          storage);
        storage.setHashTree(hTree);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    private static ByteArray randomByteArray() {
        byte[] random = new byte[16];
        RANDOM.nextBytes(random);
        return new ByteArray(random);
    }

    @Test
    public void testUpdateWithEmptyTree() {
        int noOfSegDataBlocks = 1024;
        int noOfChildren = 2;

        HTreeComponents localHTreeComp = createHashTreeAndStorage(noOfSegDataBlocks, noOfChildren);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(noOfSegDataBlocks, noOfChildren);

        for(int i = 1; i <= 2 * noOfSegDataBlocks; i++) {
            localHTreeComp.storage.put(randomByteArray(), randomByteArray());
        }

        localHTreeComp.hTree.updateHashTrees();
        boolean anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

        Assert.assertTrue(anyUpdates);
    }

    @Test
    public void testUpdateSegmentHashesTest() {

        int noOfSegDataBlocks = 4;
        StorageImplTest testStorage = new StorageImplTest();
        Pair<HashTree, HashTreeStorage> pair = getHashTreeImpl(noOfSegDataBlocks, testStorage);
        HashTree testTree = pair.getFirst();
        HashTreeStorage testTreeStorage = pair.getSecond();

        ByteArray key = new ByteArray("1".getBytes());
        ByteArray value = new ByteArray(randomBytes());
        testTree.hPut(key, value);
        ByteArray digest = new ByteArray(ByteUtils.sha1(value.get()));

        testTree.updateHashTrees();

        StringBuffer sb = new StringBuffer();
        sb.append(new ByteArray(Bytes.concat(key.get(), digest.get())) + "\n");
        ByteArray expectedLeafNodeDigest = new ByteArray(ByteUtils.sha1(sb.toString().getBytes()));
        SegmentHash segHash = testTreeStorage.getSegmentHash(1, 2);
        Assert.assertNotNull(segHash);
        Assert.assertEquals(expectedLeafNodeDigest, segHash.getHash());

        sb.setLength(0);
        sb.append(ByteUtils.toHexString(expectedLeafNodeDigest.get()) + "\n");
        ByteArray expectedRootNodeDigest = new ByteArray(ByteUtils.sha1(sb.toString().getBytes()));
        SegmentHash actualRootNodeDigest = testTreeStorage.getSegmentHash(1, 0);
        Assert.assertNotNull(actualRootNodeDigest);
        Assert.assertEquals(expectedRootNodeDigest, actualRootNodeDigest.getHash());
    }
}
