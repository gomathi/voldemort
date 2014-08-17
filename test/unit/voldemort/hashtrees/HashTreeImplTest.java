/*
 * Copyright 2008-2014 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.hashtrees;

import static voldemort.hashtrees.HashTreeImplTestUtils.createHashTreeAndStorage;
import static voldemort.hashtrees.HashTreeImplTestUtils.generateInMemoryAndPersistentStores;
import static voldemort.hashtrees.HashTreeImplTestUtils.generateInMemoryStore;
import static voldemort.hashtrees.HashTreeImplTestUtils.randomByteBuffer;
import static voldemort.hashtrees.HashTreeImplTestUtils.randomBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.Test;

import voldemort.hashtrees.HashTreeImplTestUtils.HTreeComponents;
import voldemort.hashtrees.HashTreeImplTestUtils.HashTreeIdProviderTest;
import voldemort.hashtrees.HashTreeImplTestUtils.SegIdProviderTest;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.tasks.BGHashTreeServer;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.utils.ByteUtils;

public class HashTreeImplTest {

    private static final SegIdProviderTest segIdProvider = new SegIdProviderTest();
    private static final HashTreeIdProviderTest treeIdProvider = new HashTreeIdProviderTest();
    private static final int ROOT_NODE = 0;
    private static final int DEFAULT_TREE_ID = 1;
    private static final int DEFAULT_SEG_DATA_BLOCKS_COUNT = 1 << 10;

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

        int noOfSegDataBlocks = 2;
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

            testTree.updateHashTrees(false);

            StringBuffer sb = new StringBuffer();
            ByteBuffer digest = ByteBuffer.wrap(ByteUtils.sha1(value.array()));
            sb.append(HashTreeImpl.getHexString(key, digest) + "\n");
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
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
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
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
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
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
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
                                                                      stores[j]);
            HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
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
    public void testHashTreeServerAndClient() throws TException, InterruptedException {
        HashTreeStorage store = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);
        HashTreeStorage remoteStore = generateInMemoryStore(DEFAULT_SEG_DATA_BLOCKS_COUNT);

        HTreeComponents localHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                  store);
        HTreeComponents remoteHTreeComp = createHashTreeAndStorage(DEFAULT_SEG_DATA_BLOCKS_COUNT,
                                                                   remoteStore);

        BGHashTreeServer server = new BGHashTreeServer(remoteHTreeComp.hTree,
                                                       HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO);
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
