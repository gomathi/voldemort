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
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.hashtrees.storage.HashTreePersistentStorage;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

public class HashTreePersistentStorageTest {

    private String dbDir;
    private HashTreePersistentStorage dbObj;
    private int treeId = 1;
    private int segId = 0;

    @Before
    public void init() throws Exception {
        dbDir = "/tmp/random" + new Random().nextInt();
        dbObj = new HashTreePersistentStorage(dbDir, 1024);
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

        dbObj.putSegmentData(treeId, segId, key, digest);

        SegmentData sd = dbObj.getSegmentData(treeId, segId, key);
        Assert.assertNotNull(sd);
        Assert.assertEquals(digest, sd.digest);

        dbObj.deleteSegmentData(treeId, segId, key);
        sd = dbObj.getSegmentData(treeId, segId, key);
        Assert.assertNull(sd);

        dbObj.deleteTree(treeId);
    }

    @Test
    public void testSegment() {
        List<SegmentData> list = new ArrayList<SegmentData>();
        SegmentData sd;
        for(int i = 0; i < 10; i++) {
            sd = new SegmentData(ByteBuffer.wrap(("test" + i).getBytes()),
                                 ByteBuffer.wrap(("value" + i).getBytes()));
            list.add(sd);
            dbObj.putSegmentData(treeId, segId, sd.key, sd.digest);
        }

        List<SegmentData> actualResult = dbObj.getSegment(treeId, segId);
        Assert.assertNotNull(actualResult);
        Assert.assertTrue(actualResult.size() != 0);
        Assert.assertEquals(list, actualResult);

        dbObj.deleteTree(treeId);
    }

    @Test
    public void testPutSegmentHash() {
        ByteBuffer digest = ByteBuffer.wrap("digest1".getBytes());
        dbObj.putSegmentHash(treeId, segId, digest);

        SegmentHash sh = dbObj.getSegmentHash(treeId, segId);
        Assert.assertNotNull(sh);
        Assert.assertEquals(digest, sh.hash);

        List<SegmentHash> expected = new ArrayList<SegmentHash>();
        expected.add(sh);

        List<Integer> nodeIds = new ArrayList<Integer>();
        nodeIds.add(segId);

        List<SegmentHash> actual = dbObj.getSegmentHashes(treeId, nodeIds);
        Assert.assertNotNull(actual);

        Assert.assertEquals(expected, actual);
        dbObj.deleteTree(treeId);
    }

    @Test
    public void testDeleteTree() {
        ByteBuffer key = ByteBuffer.wrap("key1".getBytes());
        ByteBuffer digest = ByteBuffer.wrap("digest1".getBytes());

        dbObj.putSegmentData(treeId, segId, key, digest);
        dbObj.deleteTree(treeId);

        SegmentData sd = dbObj.getSegmentData(treeId, segId, key);
        Assert.assertNull(sd);
    }

    @Test
    public void testSetLastFullyTreeBuiltTimestamp() {
        long exTs = System.currentTimeMillis();
        dbObj.setLastFullyTreeBuiltTimestamp(treeId, exTs);
        long dbTs = dbObj.getLastFullyTreeReBuiltTimestamp(treeId);
        Assert.assertEquals(exTs, dbTs);
    }

    @Test
    public void testLastHashTreeUpdatedTimestamp() {
        long exTs = System.currentTimeMillis();
        dbObj.setLastHashTreeUpdatedTimestamp(treeId, exTs);
        long dbTs = dbObj.getLastHashTreeUpdatedTimestamp(treeId);
        Assert.assertEquals(exTs, dbTs);
    }
}
