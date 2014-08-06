package voldemort.hashtrees;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.Test;

import voldemort.hashtrees.HashTreeImpl.SegmentIdProvider;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;

import com.google.common.primitives.Bytes;

public class HashTreeImplTest {

    private static class SegIdProviderTest implements SegmentIdProvider {

        @Override
        public int getSegmentId(ByteBuffer key) {
            return Integer.parseInt(ByteUtils.getString(key.array(), "UTF-8"));
        }

    }

    private static class HashTreeIdProviderTest implements HashTreeIdProvider {

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

    private static class StorageImplTest implements Storage {

        private final Map<ByteBuffer, ByteBuffer> localStorage = new HashMap<ByteBuffer, ByteBuffer>();
        private volatile HashTree hashTree;

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

    private static ByteBuffer randomByteBuffer() {
        byte[] random = new byte[8];
        RANDOM.nextBytes(random);
        return ByteBuffer.wrap(random);
    }

    private static String randomDirName() {
        return "/tmp/test/random" + RANDOM.nextInt();
    }

    private static HTreeComponents createHashTreeAndStorage(int noOfSegDataBlocks,
                                                            final HashTreeIdProvider treeIdProv,
                                                            final SegmentIdProvider segIdPro,
                                                            final HashTreeStorage hTStorage) {
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegDataBlocks,
                                          treeIdProv,
                                          segIdPro,
                                          hTStorage,
                                          storage);
        hTree.updateHashTrees(false);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    private static HTreeComponents createHashTreeAndStorage(int noOfSegments,
                                                            int noOfChildrenPerParent,
                                                            final HashTreeStorage hTStorage) {
        HashTreeIdProvider treeIdProvider = new HashTreeIdProviderTest();
        StorageImplTest storage = new StorageImplTest();
        HashTree hTree = new HashTreeImpl(noOfSegments,
                                          noOfChildrenPerParent,
                                          treeIdProvider,
                                          hTStorage,
                                          storage);
        storage.setHashTree(hTree);
        hTree.updateHashTrees(false);
        return new HTreeComponents(hTStorage, storage, hTree);
    }

    private static HashTreeStorage generateInMemoryStore(int noOfSegDataBlocks) {
        return new HashTreeStorageInMemory(noOfSegDataBlocks);
    }

    private static HashTreeStorage generatePersistentStore(int noOfSegDataBlocks)
            throws IOException {
        return new HashTreePersistentStorage(randomDirName(), noOfSegDataBlocks);
    }

    private static HashTreeStorage[] generateInMemoryAndPersistentStores(int noOfSegDataBlocks)
            throws IOException {
        HashTreeStorage[] stores = new HashTreeStorage[2];
        stores[0] = generateInMemoryStore(noOfSegDataBlocks);
        stores[1] = generatePersistentStore(noOfSegDataBlocks);
        return stores;
    }

    @Test
    public void testPut() throws IOException {

        int noOfSegDataBlocks = 1024;
        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(noOfSegDataBlocks);

        for(HashTreeStorage store: stores) {

            HTreeComponents components = createHashTreeAndStorage(noOfSegDataBlocks,
                                                                  treeIdProvider,
                                                                  segIdProvider,
                                                                  store);
            HashTree testTree = components.hTree;
            HashTreeStorage testTreeStorage = components.hTStorage;

            ByteBuffer key = ByteBuffer.wrap("1".getBytes());
            ByteBuffer value = ByteBuffer.wrap(randomBytes());
            testTree.hPut(key, value);
            ByteBuffer digest = ByteBuffer.wrap(ByteUtils.sha1(value.array()));

            SegmentData segData = testTreeStorage.getSegmentData(1, 1, key);
            Assert.assertNotNull(segData);
            Assert.assertTrue(Arrays.equals(key.array(), segData.getKey()));
            Assert.assertTrue(Arrays.equals(digest.array(), segData.getDigest()));

            List<Integer> dirtySegs = testTreeStorage.clearAndGetDirtySegments(1);
            Assert.assertEquals(1, dirtySegs.size());
            Assert.assertEquals(1, dirtySegs.get(0).intValue());
        }
    }

    @Test
    public void testRemove() throws IOException {

        int noOfSegDataBlocks = 1024;
        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(noOfSegDataBlocks);

        for(HashTreeStorage store: stores) {
            HTreeComponents components = createHashTreeAndStorage(noOfSegDataBlocks,
                                                                  treeIdProvider,
                                                                  segIdProvider,
                                                                  store);
            HashTree testTree = components.hTree;
            HashTreeStorage testTreeStorage = components.hTStorage;

            ByteBuffer key = ByteBuffer.wrap("2".getBytes());
            ByteBuffer value = ByteBuffer.wrap(randomBytes());
            testTree.hPut(key, value);
            testTree.hRemove(key);

            SegmentData segData = testTreeStorage.getSegmentData(1, 2, key);
            Assert.assertNull(segData);

            List<Integer> dirtySegs = testTreeStorage.clearAndGetDirtySegments(1);
            Assert.assertEquals(1, dirtySegs.size());
            Assert.assertEquals(2, dirtySegs.get(0).intValue());
        }
    }

    @Test
    public void testUpdateSegmentHashesTest() throws IOException {

        int noOfSegDataBlocks = 4;
        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(noOfSegDataBlocks);

        for(HashTreeStorage store: stores) {
            HTreeComponents components = createHashTreeAndStorage(noOfSegDataBlocks,
                                                                  treeIdProvider,
                                                                  segIdProvider,
                                                                  store);
            HashTree testTree = components.hTree;
            HashTreeStorage testTreeStorage = components.hTStorage;

            ByteBuffer key = ByteBuffer.wrap("1".getBytes());
            ByteBuffer value = ByteBuffer.wrap(randomBytes());
            testTree.hPut(key, value);
            ByteBuffer digest = ByteBuffer.wrap(ByteUtils.sha1(value.array()));

            testTree.updateHashTrees(false);

            StringBuffer sb = new StringBuffer();
            sb.append(ByteUtils.toHexString(Bytes.concat(key.array(), digest.array())) + "\n");
            byte[] expectedLeafNodeDigest = ByteUtils.sha1(sb.toString().getBytes());
            SegmentHash segHash = testTreeStorage.getSegmentHash(1, 2);
            Assert.assertNotNull(segHash);
            Assert.assertTrue(Arrays.equals(expectedLeafNodeDigest, segHash.getHash()));

            sb.setLength(0);
            sb.append(ByteUtils.toHexString(expectedLeafNodeDigest) + "\n");
            byte[] expectedRootNodeDigest = ByteUtils.sha1(sb.toString().getBytes());
            SegmentHash actualRootNodeDigest = testTreeStorage.getSegmentHash(1, 0);
            Assert.assertNotNull(actualRootNodeDigest);
            Assert.assertTrue(Arrays.equals(expectedRootNodeDigest, actualRootNodeDigest.getHash()));
        }
    }

    @Test
    public void testUpdateWithEmptyTree() throws IOException, TException {
        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage[] remoteStores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        for(int j = 0; j <= 1; j++) {
            HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                      DEFAULT_NO_OF_CHILDREN,
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                       DEFAULT_NO_OF_CHILDREN,
                                                                       remoteStores[j]);

            for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                localHTreeComp.storage.put(randomByteBuffer(), randomByteBuffer());
            }

            localHTreeComp.hTree.updateHashTrees(false);
            boolean anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
            Assert.assertTrue(anyUpdates);

            remoteHTreeComp.hTree.updateHashTrees(false);
            anyUpdates = localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
            Assert.assertFalse(anyUpdates);

            SegmentHash localRootHash = localHTreeComp.hTree.getSegmentHash(DEFAULT_TREE_ID,
                                                                            ROOT_NODE);
            Assert.assertNotNull(localRootHash);
            SegmentHash remoteRootHash = remoteHTreeComp.hTree.getSegmentHash(DEFAULT_TREE_ID,
                                                                              ROOT_NODE);
            Assert.assertNotNull(remoteRootHash);

            Assert.assertTrue(Arrays.equals(localRootHash.getHash(), remoteRootHash.getHash()));
        }
    }

    @Test
    public void testUpdateTreeWithMissingBlocksInLocal() throws IOException, TException {

        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage[] remoteStores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        for(int j = 0; j <= 1; j++) {
            HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                      DEFAULT_NO_OF_CHILDREN,
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                       DEFAULT_NO_OF_CHILDREN,
                                                                       remoteStores[j]);

            for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                localHTreeComp.storage.put(randomByteBuffer(), randomByteBuffer());
            }

            localHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

            for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
                for(SegmentData sData: segBlock) {
                    localHTreeComp.storage.remove(ByteBuffer.wrap(sData.getKey()));
                }
                localHTreeComp.hTree.updateHashTrees(false);
                remoteHTreeComp.hTree.updateHashTrees(false);
                localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

                Assert.assertEquals(localHTreeComp.storage.localStorage,
                                    remoteHTreeComp.storage.localStorage);
            }

            Assert.assertEquals(0, localHTreeComp.storage.localStorage.size());
            Assert.assertEquals(0, remoteHTreeComp.storage.localStorage.size());
        }
    }

    @Test
    public void testUpdateTreeWithMissingBlocksInRemote() throws IOException, TException {

        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage[] remoteStores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        for(int j = 0; j <= 1; j++) {
            HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                      DEFAULT_NO_OF_CHILDREN,
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                       DEFAULT_NO_OF_CHILDREN,
                                                                       remoteStores[j]);

            for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                localHTreeComp.storage.put(randomByteBuffer(), randomByteBuffer());
            }

            localHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);
            remoteHTreeComp.hTree.updateHashTrees(false);

            for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
                for(SegmentData sData: segBlock) {
                    remoteHTreeComp.storage.remove(ByteBuffer.wrap(sData.getKey()));
                }
                remoteHTreeComp.hTree.updateHashTrees(false);
                localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

                Assert.assertEquals(localHTreeComp.storage.localStorage,
                                    remoteHTreeComp.storage.localStorage);
            }
        }
    }

    @Test
    public void testUpdateTreeWithDifferingSegments() throws IOException, TException {
        HashTreeStorage[] stores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage[] remoteStores = generateInMemoryAndPersistentStores(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        for(int j = 0; j <= 1; j++) {
            HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                      DEFAULT_NO_OF_CHILDREN,
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                       DEFAULT_NO_OF_CHILDREN,
                                                                       remoteStores[j]);

            for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                localHTreeComp.storage.put(randomByteBuffer(), randomByteBuffer());
            }

            localHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

            for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
                List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
                for(SegmentData sData: segBlock) {
                    localHTreeComp.storage.put(ByteBuffer.wrap(sData.getKey()), randomByteBuffer());
                }
                localHTreeComp.hTree.updateHashTrees(false);
                remoteHTreeComp.hTree.updateHashTrees(false);
                localHTreeComp.hTree.synch(1, remoteHTreeComp.hTree);

                Assert.assertEquals(localHTreeComp.storage.localStorage,
                                    remoteHTreeComp.storage.localStorage);
            }
        }
    }

    @Test
    public void testThriftServerAndClient() throws TException, InterruptedException {
        HashTreeStorage store = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage remoteStore = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  DEFAULT_NO_OF_CHILDREN,
                                                                  store);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                   DEFAULT_NO_OF_CHILDREN,
                                                                   remoteStore);

        BGHashTreeServer server = new BGHashTreeServer(new CountDownLatch(1),
                                                       HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO,
                                                       remoteHTreeComp.hTree);
        new Thread(server).start();

        Thread.sleep(100);

        HashTreeSyncInterface.Iface client = HashTreeClientGenerator.getHashTreeClient("localhost");

        for(int i = 1; i <= DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            localHTreeComp.storage.put(randomByteBuffer(), randomByteBuffer());
        }

        localHTreeComp.hTree.updateHashTrees(false);
        localHTreeComp.hTree.synch(1, client);

        for(int i = 0; i < DEFAULT_SEG_DATA_BLOCKS_COUNT; i++) {
            List<SegmentData> segBlock = remoteHTreeComp.hTree.getSegment(DEFAULT_TREE_ID, i);
            for(SegmentData sData: segBlock) {
                localHTreeComp.storage.put(ByteBuffer.wrap(sData.getKey()), randomByteBuffer());
            }
            localHTreeComp.hTree.updateHashTrees(false);
            remoteHTreeComp.hTree.updateHashTrees(false);
            localHTreeComp.hTree.synch(1, client);

            Assert.assertEquals(localHTreeComp.storage.localStorage,
                                remoteHTreeComp.storage.localStorage);
        }
    }
}
