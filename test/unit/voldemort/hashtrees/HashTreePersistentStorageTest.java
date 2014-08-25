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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.actors.threadpool.Arrays;
import voldemort.hashtrees.storage.HTPersistentStorage;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.hashtrees.thrift.generated.VersionedData;
import voldemort.utils.Pair;

public class HashTreePersistentStorageTest {

    private String dbDir;
    private int defaultTreeId = 1;
    private int versionedTreeId = 5;
    private int defaultSegId = 0;
    private int noOfSegDataBlocks = 1024;
    private HTPersistentStorage dbObj;

    @Before
    public void init() throws Exception {
        dbDir = "/tmp/random" + new Random().nextInt();
        dbObj = new HTPersistentStorage(dbDir, noOfSegDataBlocks);
    }

    public void init(String dbDirName) throws Exception {
        dbObj = new HTPersistentStorage(dbDirName, noOfSegDataBlocks);
    }

    @After
    public void deleteDBDir() {
        if(dbObj != null)
            dbObj.close();
        File dbDirObj = new File(dbDir);
        if(dbDirObj.exists())
            FileUtils.deleteQuietly(dbDirObj);
    }

    @Test
    public void testSegmentData() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        ByteBuffer digest = ByteBuffer.wrap("digest1".getBytes());

        dbObj.putSegmentData(defaultTreeId, defaultSegId, key, digest);

        SegmentData sd = dbObj.getSegmentData(defaultTreeId, defaultSegId, key);
        Assert.assertNotNull(sd);
        Assert.assertEquals(digest, sd.digest);

        dbObj.deleteSegmentData(defaultTreeId, defaultSegId, key);
        sd = dbObj.getSegmentData(defaultTreeId, defaultSegId, key);
        Assert.assertNull(sd);

        dbObj.deleteTree(defaultTreeId);
    }

    @Test
    public void testSegment() {
        List<SegmentData> list = new ArrayList<SegmentData>();
        SegmentData sd;
        for(int i = 0; i < 10; i++) {
            sd = new SegmentData(ByteBuffer.wrap(("test" + i).getBytes()),
                                 ByteBuffer.wrap(("value" + i).getBytes()));
            list.add(sd);
            dbObj.putSegmentData(defaultTreeId, defaultSegId, sd.key, sd.digest);
        }

        List<SegmentData> actualResult = dbObj.getSegment(defaultTreeId, defaultSegId);
        Assert.assertNotNull(actualResult);
        Assert.assertTrue(actualResult.size() != 0);
        Assert.assertEquals(list, actualResult);

        dbObj.deleteTree(defaultTreeId);
    }

    @Test
    public void testPutSegmentHash() {
        ByteBuffer digest = ByteBuffer.wrap("digest1".getBytes());
        dbObj.putSegmentHash(defaultTreeId, defaultSegId, digest);

        SegmentHash sh = dbObj.getSegmentHash(defaultTreeId, defaultSegId);
        Assert.assertNotNull(sh);
        Assert.assertEquals(digest, sh.hash);

        List<SegmentHash> expected = new ArrayList<SegmentHash>();
        expected.add(sh);

        List<Integer> nodeIds = new ArrayList<Integer>();
        nodeIds.add(defaultSegId);

        List<SegmentHash> actual = dbObj.getSegmentHashes(defaultTreeId, nodeIds);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected, actual);
        dbObj.deleteTree(defaultTreeId);
    }

    @Test
    public void testDeleteTree() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        ByteBuffer digest = ByteBuffer.wrap("digest1".getBytes());

        dbObj.putSegmentData(defaultTreeId, defaultSegId, key, digest);
        dbObj.deleteTree(defaultTreeId);

        SegmentData sd = dbObj.getSegmentData(defaultTreeId, defaultSegId, key);
        Assert.assertNull(sd);
    }

    @Test
    public void testSetLastFullyTreeBuiltTimestamp() {
        long exTs = System.currentTimeMillis();
        dbObj.setLastFullyTreeBuiltTimestamp(defaultTreeId, exTs);
        long dbTs = dbObj.getLastFullyTreeReBuiltTimestamp(defaultTreeId);
        Assert.assertEquals(exTs, dbTs);
    }

    @Test
    public void testLastHashTreeUpdatedTimestamp() {
        long exTs = System.currentTimeMillis();
        dbObj.setLastHashTreeUpdatedTimestamp(defaultTreeId, exTs);
        long dbTs = dbObj.getLastHashTreeUpdatedTimestamp(defaultTreeId);
        Assert.assertEquals(exTs, dbTs);
    }

    private List<Pair<ByteBuffer, ByteBuffer>> generateRandomKeyValueList(int count) {
        List<Pair<ByteBuffer, ByteBuffer>> result = new ArrayList<Pair<ByteBuffer, ByteBuffer>>();
        for(int i = 0; i < count; i++)
            result.add(Pair.create(HashTreeImplTestUtils.randomByteBuffer(),
                                   HashTreeImplTestUtils.randomByteBuffer()));
        return result;
    }

    private List<Pair<ByteBuffer, ByteBuffer>> generateRandomKeyList(int count) {
        List<Pair<ByteBuffer, ByteBuffer>> result = new ArrayList<Pair<ByteBuffer, ByteBuffer>>();
        ByteBuffer value = null;
        for(int i = 0; i < count; i++)
            result.add(Pair.create(HashTreeImplTestUtils.randomByteBuffer(), value));
        return result;
    }

    @Test
    public void testVersionedData() throws Exception {
        List<Pair<ByteBuffer, ByteBuffer>> expected = generateRandomKeyValueList(10);
        expected.addAll(generateRandomKeyList(10));
        Collections.shuffle(expected);

        writeData(expected);
        Iterator<VersionedData> itr = dbObj.getVersionedData(versionedTreeId);
        List<Pair<ByteBuffer, ByteBuffer>> actual = readData(itr);
        validate(expected, actual);

        writeData(expected);
        long versionNo = expected.size() + 1;
        itr = dbObj.getVersionedData(versionedTreeId, versionNo);
        actual = readData(itr);
        validate(expected, actual);

        dbObj.close();
        init(dbDir);

        versionNo = expected.size() + 1;
        dbObj.putVersionedDataToAdditionList(versionedTreeId,
                                             HashTreeImplTestUtils.randomByteBuffer(),
                                             HashTreeImplTestUtils.randomByteBuffer());

        itr = dbObj.getVersionedData(versionedTreeId, versionNo);
        Assert.assertTrue(itr.hasNext());
        VersionedData vData = itr.next();
        Assert.assertEquals(versionNo, vData.getVersionNo());

        dbObj.deleteTree(versionedTreeId);
    }

    private void writeData(List<Pair<ByteBuffer, ByteBuffer>> expected) {
        for(Pair<ByteBuffer, ByteBuffer> pair: expected) {
            if(pair.getSecond() == null)
                dbObj.putVersionedDataToRemovalList(versionedTreeId, pair.getFirst());
            else
                dbObj.putVersionedDataToAdditionList(versionedTreeId,
                                                     pair.getFirst(),
                                                     pair.getSecond());
        }
    }

    private List<Pair<ByteBuffer, ByteBuffer>> readData(Iterator<VersionedData> itr) {
        ByteBuffer value = null;
        List<Pair<ByteBuffer, ByteBuffer>> actual = new ArrayList<Pair<ByteBuffer, ByteBuffer>>();
        while(itr.hasNext()) {
            VersionedData vData = itr.next();
            if(vData.addedOrRemoved)
                actual.add(Pair.create(vData.key, vData.value));
            else
                actual.add(Pair.create(vData.key, value));
        }
        return actual;
    }

    private void validate(List<Pair<ByteBuffer, ByteBuffer>> expected,
                          List<Pair<ByteBuffer, ByteBuffer>> actual) {
        if(expected == null && actual == null)
            return;
        if(expected == null || actual == null)
            Assert.assertTrue(false);

        Assert.assertEquals(expected.size(), actual.size());

        for(int i = 0; i < expected.size(); i++) {
            Assert.assertTrue(Arrays.equals(expected.get(i).getFirst().array(), actual.get(i)
                                                                                      .getFirst()
                                                                                      .array()));
            if(expected.get(i).getSecond() == null)
                Assert.assertEquals(expected.get(i).getSecond(), actual.get(i).getSecond());
            else
                Assert.assertTrue(Arrays.equals(expected.get(i).getSecond().array(),
                                                actual.get(i).getSecond().array()));
        }
    }
}
