package voldemort.hashtrees;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

        private final Map<ByteArray, ByteArray> localStorage = new HashMap<ByteArray, ByteArray>();
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
        public ByteArray remove(ByteArray key) {
            ByteArray value = localStorage.remove(key);
            if(hashTree != null) {
                hashTree.hRemove(key);
                return value;
            }
            return null;
        }

        @Override
        public Iterator<Pair<ByteArray, ByteArray>> iterator() {
            return null;
        }

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

    private static final SegIdProviderTest segIdProvider = new SegIdProviderTest();
    private static final HashTreeIdProviderTest treeIdProvider = new HashTreeIdProviderTest();
    private static final Random RANDOM = new Random(System.currentTimeMillis());
    private static final int ROOT_NODE = 0;
    private static final int DEFAULT_TREE_ID = 1;
    private static final int DEFAULT_SEG_DATA_BLOCKS_COUNT = 1 << 10;
    private static final int DEFAULT_NO_OF_CHILDREN = 2;

    private static byte[] randomBytes() {
        byte[] emptyBuffer = new byte[8];
        RANDOM.nextBytes(emptyBuffer);
        return emptyBuffer;
    }

    private static ByteArray randomByteArray() {
        byte[] random = new byte[8];
        RANDOM.nextBytes(random);
        return new ByteArray(random);
    }

    private static HTreeComponents createHashTreeAndStorage(int noOfSegDataBlocks,
                                                            HashTreeIdProvider treeIdProv,
                                                            SegmentIdProvider segIdPro) {
        HashTreeStorage hTStorage;
        try {
            hTStorage = new HashTreePersistentStorage("/tmp/test/random.txt" + RANDOM.nextInt(),
                                                      noOfSegDataBlocks);
        } catch(IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException(e);
        }
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegDataBlocks,
                                          treeIdProv,
                                          segIdPro,
                                          hTStorage,
                                          storage);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    private static HTreeComponents createHashTreeAndStorage(int noOfSegments,
                                                            int noOfChildrenPerParent) {
        HashTreeIdProvider treeIdProvider = new HashTreeIdProviderTest();
        HashTreeStorage hTStorage;
        try {
            hTStorage = new HashTreePersistentStorage("/tmp/test/random.txt" + RANDOM.nextInt(),
                                                      noOfSegments);
        } catch(IOException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException();
        }
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegments,
                                          noOfChildrenPerParent,
                                          treeIdProvider,
                                          hTStorage,
                                          storage);
        storage.setHashTree(hTree);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    @Test
    public void testPut() {

        int noOfSegDataBlocks = 1024;
        HTreeComponents components = createHashTreeAndStorage(noOfSegDataBlocks,
                                                              treeIdProvider,
                                                              segIdProvider);
        HashTree testTree = components.hTree;
        HashTreeStorage testTreeStorage = components.hTStorage;

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
        HTreeComponents components = createHashTreeAndStorage(noOfSegDataBlocks,
                                                              treeIdProvider,
                                                              segIdProvider);
        HashTree testTree = components.hTree;
        HashTreeStorage testTreeStorage = components.hTStorage;

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

    @Test
    public void testUpdateSegmentHashesTest() {

        int noOfSegDataBlocks = 4;
        HTreeComponents components = createHashTreeAndStorage(noOfSegDataBlocks,
                                                              treeIdProvider,
                                                              segIdProvider);
        HashTree testTree = components.hTree;
        HashTreeStorage testTreeStorage = components.hTStorage;

        ByteArray key = new ByteArray("1".getBytes());
        ByteArray value = new ByteArray(randomBytes());
        testTree.hPut(key, value);
        ByteArray digest = new ByteArray(ByteUtils.sha1(value.get()));

        testTree.updateHashTrees(false);

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

    @Test
    public void testUpdateWithEmptyTree() {
        HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  DEFAULT_NO_OF_CHILDREN);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                   DEFAULT_NO_OF_CHILDREN);

        for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            localHTreeComp.storage.put(randomByteArray(), randomByteArray());
        }

        localHTreeComp.hTree.updateHashTrees(false);
        boolean anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
        Assert.assertTrue(anyUpdates);

        remoteHTreeComp.hTree.updateHashTrees(false);
        anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
        Assert.assertFalse(anyUpdates);

        SegmentHash localRootHash = localHTreeComp.hTree.getSegmentHash(DEFAULT_TREE_ID, ROOT_NODE);
        Assert.assertNotNull(localRootHash);
        SegmentHash remoteRootHash = remoteHTreeComp.hTree.getSegmentHash(DEFAULT_TREE_ID,
                                                                          ROOT_NODE);
        Assert.assertNotNull(remoteRootHash);

        Assert.assertTrue(localRootHash.getHash().equals(remoteRootHash.getHash()));
    }

    @Test
    public void testUpdateTreeWithMissingBlocksInLocal() {

        HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  DEFAULT_NO_OF_CHILDREN);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                   DEFAULT_NO_OF_CHILDREN);

        for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            localHTreeComp.storage.put(randomByteArray(), randomByteArray());
        }

        localHTreeComp.hTree.updateHashTrees(false);
        localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

        for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
            for(SegmentData sData: segBlock) {
                localHTreeComp.storage.remove(sData.getKey());
            }
            localHTreeComp.hTree.updateHashTrees(false);
            remoteHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

            Assert.assertEquals(localHTreeComp.storage.localStorage,
                                remoteHTreeComp.storage.localStorage);
        }

        Assert.assertTrue(localHTreeComp.storage.localStorage.size() == 0);
        Assert.assertTrue(remoteHTreeComp.storage.localStorage.size() == 0);
    }

    @Test
    public void testUpdateTreeWithMissingBlocksInRemote() {
        HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  DEFAULT_NO_OF_CHILDREN);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                   DEFAULT_NO_OF_CHILDREN);

        for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            localHTreeComp.storage.put(randomByteArray(), randomByteArray());
        }

        localHTreeComp.hTree.updateHashTrees(false);
        localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
        remoteHTreeComp.hTree.updateHashTrees(false);

        for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
            for(SegmentData sData: segBlock) {
                remoteHTreeComp.storage.remove(sData.getKey());
            }
            remoteHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

            Assert.assertEquals(localHTreeComp.storage.localStorage,
                                remoteHTreeComp.storage.localStorage);
        }
    }

    @Test
    public void testUpdateTreeWithDifferingSegments() {
        HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  DEFAULT_NO_OF_CHILDREN);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                   DEFAULT_NO_OF_CHILDREN);

        for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            localHTreeComp.storage.put(randomByteArray(), randomByteArray());
        }

        localHTreeComp.hTree.updateHashTrees(false);
        localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

        for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
            for(SegmentData sData: segBlock) {
                localHTreeComp.storage.put(sData.getKey(), randomByteArray());
            }
            localHTreeComp.hTree.updateHashTrees(false);
            remoteHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

            Assert.assertEquals(localHTreeComp.storage.localStorage,
                                remoteHTreeComp.storage.localStorage);
        }
    }
}
