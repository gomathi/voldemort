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
package voldemort.hashtrees.storage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.hashtrees.thrift.generated.VersionedData;
import voldemort.utils.AtomicBitSet;
import voldemort.utils.ByteUtils;

/**
 * Uses LevelDB for storing segment hashes and segment data. Dirty segment
 * markers are stored in memory.
 * 
 * Stores the following data
 * 
 * 1) Metadata info [Like when the tree was built fully last time]. Format is
 * ['M'|key] -> [value] 2) SegmentData, format is ['S'|treeId|segId|key] ->
 * [value] 3) SegmentHash, format is ['H'|treeId|nodeId] -> [value] 4) Put
 * versioned (key,value) pairs into the database.
 * ['V'|versionNo|[Addition|Removal]|datakey] -> [value] pair.
 * 
 */

public class HashTreePersistentStorage implements HashTreeStorage {

    private static final Logger LOG = Logger.getLogger(HashTreePersistentStorage.class);

    private static final byte[] KEY_LAST_FULLY_TREE_BUILT_TS = "ltfbTs".getBytes();
    private static final byte[] KEY_LAST_TREE_BUILT_TS = "ltbTs".getBytes();

    private static final byte META_DATA_MARKER = 'M';
    private static final byte SEG_HASH_MARKER = 'H';
    private static final byte SEG_DATA_MARKER = 'S';

    private static final byte KEY_VERSIONED_DATA_PREFIX = 'V';
    private static final byte ADDITION_MARKER = 'A';
    private static final byte REMOVAL_MARKER = 'R';
    private static final byte[] DUMMY_ENTRY = { 'd' };

    private static final byte[] KEY_PREFIX_ARRAY = { META_DATA_MARKER, SEG_DATA_MARKER,
            SEG_HASH_MARKER };

    private final DB dbObj;
    private final int noOfSegDataBlocks;
    private final ConcurrentMap<Integer, AtomicBitSet> treeIdAndDirtySegmentMap = new ConcurrentHashMap<Integer, AtomicBitSet>();
    private final AtomicLong versionNo;

    public HashTreePersistentStorage(String dbDir, int noOfSegDataBlocks) throws Exception {
        this.dbObj = initDatabase(dbDir);
        this.noOfSegDataBlocks = noOfSegDataBlocks;
        this.versionNo = new AtomicLong(loadVersionNo());
        loadVersionNo();
    }

    private static boolean createDir(String dirName) {
        File file = new File(dirName);
        if(file.exists())
            return true;
        return file.mkdirs();
    }

    private static DB initDatabase(String dbDir) throws IOException {
        createDir(dbDir);
        Options options = new Options();
        options.createIfMissing(true);
        return new JniDBFactory().open(new File(dbDir), options);
    }

    private long loadVersionNo() {
        byte[] keyPrefix = new byte[1];
        ByteBuffer keyPrefixBuf = ByteBuffer.wrap(keyPrefix);
        byte nextKeyVersionedDataPrefix = 'V' + 1;
        keyPrefixBuf.put(nextKeyVersionedDataPrefix);
        DBIterator itr = dbObj.iterator();
        itr.seek(keyPrefix);
        if(itr.hasPrev()) {
            byte[] key = itr.peekPrev().getKey();
            long versionNo = ByteUtils.readLong(key, keyPrefix.length);
            return versionNo;
        }
        return 0;
    }

    public void close() {
        try {
            dbObj.close();
        } catch(IOException e) {
            LOG.warn("Exception occurred while closing leveldb connection.");
        }
    }

    private AtomicBitSet getDirtySegmentsHolder(int treeId) {
        if(!treeIdAndDirtySegmentMap.containsKey(treeId))
            treeIdAndDirtySegmentMap.putIfAbsent(treeId, new AtomicBitSet(noOfSegDataBlocks));
        return treeIdAndDirtySegmentMap.get(treeId);
    }

    private static void prepareKeyPrefix(ByteBuffer keyToFill, byte prefixByte, int... partialKeys) {
        keyToFill.put(prefixByte);
        for(int key: partialKeys)
            keyToFill.putInt(key);
    }

    private static byte[] prepareSegmentHashKey(int treeId, int nodeId) {
        byte[] key = new byte[1 + ByteUtils.SIZE_OF_INT * 2];
        ByteBuffer bb = ByteBuffer.wrap(key);
        prepareKeyPrefix(bb, SEG_HASH_MARKER, treeId, nodeId);
        return key;
    }

    private static byte[] readSegmentDataKey(byte[] dbSegDataKey) {
        int from = (ByteUtils.SIZE_OF_INT * 2) + 1;
        byte[] key = ByteUtils.copy(dbSegDataKey, from, dbSegDataKey.length);
        return key;
    }

    private static byte[] prepareSegmentDataKeyPrefix(int treeId, int segId) {
        byte[] byteKey = new byte[1 + ByteUtils.SIZE_OF_INT * 2];
        ByteBuffer bb = ByteBuffer.wrap(byteKey);
        prepareKeyPrefix(bb, SEG_DATA_MARKER, treeId, segId);
        return byteKey;
    }

    private static byte[] prepareSegmentDataKey(int treeId, int segId, ByteBuffer key) {
        byte[] byteKey = new byte[1 + ByteUtils.SIZE_OF_INT * 2 + (key.array().length)];
        ByteBuffer bb = ByteBuffer.wrap(byteKey);
        prepareKeyPrefix(bb, SEG_DATA_MARKER, treeId, segId);
        bb.put(key.array());
        return byteKey;
    }

    private static byte[] prepareMetaDataKey(int treeId, byte[] keyName) {
        byte[] byteKey = new byte[1 + ByteUtils.SIZE_OF_INT + keyName.length];
        ByteBuffer bb = ByteBuffer.wrap(byteKey);
        prepareKeyPrefix(bb, META_DATA_MARKER, treeId);
        bb.put(keyName);
        return byteKey;
    }

    private void updateMetaData(int treeId, byte[] partialKey, byte[] value) {
        byte[] key = prepareMetaDataKey(treeId, partialKey);
        dbObj.put(key, value);
    }

    @Override
    public void putSegmentHash(int treeId, int nodeId, ByteBuffer digest) {
        dbObj.put(prepareSegmentHashKey(treeId, nodeId), digest.array());
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) {
        byte[] value = dbObj.get(prepareSegmentHashKey(treeId, nodeId));
        if(value != null)
            return new SegmentHash(nodeId, ByteBuffer.wrap(value));
        return null;
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds) {
        List<SegmentHash> result = new ArrayList<SegmentHash>();
        SegmentHash temp;
        for(int nodeId: nodeIds) {
            temp = getSegmentHash(treeId, nodeId);
            if(temp != null)
                result.add(temp);
        }
        return result;
    }

    @Override
    public void setDirtySegment(int treeId, int segId) {
        getDirtySegmentsHolder(treeId).set(segId);
    }

    @Override
    public List<Integer> clearAndGetDirtySegments(int treeId) {
        return getDirtySegmentsHolder(treeId).clearAndGetAllSetBits();
    }

    @Override
    public void clearAllSegments(int treeId) {
        getDirtySegmentsHolder(treeId).clear();
    }

    @Override
    public void setLastFullyTreeBuiltTimestamp(int treeId, long timestamp) {
        byte[] value = new byte[ByteUtils.SIZE_OF_LONG];
        ByteBuffer bbValue = ByteBuffer.wrap(value);
        bbValue.putLong(timestamp);
        updateMetaData(treeId, KEY_LAST_FULLY_TREE_BUILT_TS, value);
    }

    @Override
    public long getLastFullyTreeReBuiltTimestamp(int treeId) {
        byte[] key = prepareMetaDataKey(treeId, KEY_LAST_FULLY_TREE_BUILT_TS);
        byte[] value = dbObj.get(key);
        if(value != null)
            return ByteUtils.readLong(value, 0);
        return 0;
    }

    @Override
    public void setLastHashTreeUpdatedTimestamp(int treeId, long timestamp) {
        byte[] value = new byte[ByteUtils.SIZE_OF_LONG];
        ByteBuffer bbValue = ByteBuffer.wrap(value);
        bbValue.putLong(timestamp);
        updateMetaData(treeId, KEY_LAST_TREE_BUILT_TS, value);
    }

    @Override
    public long getLastHashTreeUpdatedTimestamp(int treeId) {
        byte[] key = prepareMetaDataKey(treeId, KEY_LAST_TREE_BUILT_TS);
        byte[] value = dbObj.get(key);
        if(value != null)
            return ByteUtils.readLong(value, 0);
        return 0;
    }

    @Override
    public void deleteTree(int treeId) {
        DBIterator dbItr;
        byte[] temp = new byte[1 + ByteUtils.SIZE_OF_INT];
        for(byte keyPrefix: KEY_PREFIX_ARRAY) {
            dbItr = dbObj.iterator();
            ByteBuffer wrap = ByteBuffer.wrap(temp);
            prepareKeyPrefix(wrap, keyPrefix, treeId);
            dbItr.seek(wrap.array());
            for(; dbItr.hasNext(); dbItr.next()) {
                if(ByteUtils.compare(temp, dbItr.peekNext().getKey(), 0, temp.length) != 0)
                    break;
                dbObj.delete(dbItr.peekNext().getKey());
            }
        }
    }

    @Override
    public void deleteAllVersionedData() {
        byte[] rowKeyPrefix = { KEY_VERSIONED_DATA_PREFIX };
        DBIterator iterator = dbObj.iterator();
        iterator.seek(rowKeyPrefix);
        while(iterator.hasNext()) {
            byte[] rowKey = iterator.peekNext().getKey();
            if(ByteUtils.compare(rowKeyPrefix, rowKey, 0, rowKeyPrefix.length) != 0)
                break;
            dbObj.delete(rowKey);
            iterator.next();
        }
    }

    @Override
    public void putSegmentData(int treeId, int segId, ByteBuffer key, ByteBuffer digest) {
        byte[] dbKey = prepareSegmentDataKey(treeId, segId, key);
        dbObj.put(dbKey, digest.array());
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) {
        byte[] dbKey = prepareSegmentDataKey(treeId, segId, key);
        byte[] value = dbObj.get(dbKey);
        if(value != null)
            return new SegmentData(key, ByteBuffer.wrap(value));
        return null;
    }

    @Override
    public void deleteSegmentData(int treeId, int segId, ByteBuffer key) {
        byte[] dbKey = prepareSegmentDataKey(treeId, segId, key);
        dbObj.delete(dbKey);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        List<SegmentData> result = new ArrayList<SegmentData>();
        byte[] startKey = prepareSegmentDataKeyPrefix(treeId, segId);
        DBIterator iterator = dbObj.iterator();
        try {
            for(iterator.seek(startKey); iterator.hasNext(); iterator.next()) {
                if(ByteUtils.compare(startKey, iterator.peekNext().getKey(), 0, startKey.length) != 0)
                    break;
                ByteBuffer key = ByteBuffer.wrap(readSegmentDataKey(iterator.peekNext().getKey()));
                ByteBuffer digest = ByteBuffer.wrap(iterator.peekNext().getValue());
                result.add(new SegmentData(key, digest));
            }
        } finally {
            try {
                iterator.close();
            } catch(IOException e) {
                LOG.warn("Exception occurred while closing the DBIterator.", e);
            }
        }
        return result;
    }

    @Override
    public VersionedData versionedPut(ByteBuffer key, ByteBuffer value) {
        byte[] keyArr = key.array();
        byte[] fullKey = new byte[2 + ByteUtils.SIZE_OF_LONG + keyArr.length];
        ByteBuffer fullKeyBuffer = ByteBuffer.wrap(fullKey);
        fullKeyBuffer.put(KEY_VERSIONED_DATA_PREFIX);
        fullKeyBuffer.putLong(versionNo.incrementAndGet());
        fullKeyBuffer.put(ADDITION_MARKER);
        fullKeyBuffer.put(keyArr);
        dbObj.put(fullKeyBuffer.array(), value.array());

        VersionedData vData = new VersionedData(versionNo.get(), true, key);
        vData.setValue(value);
        return vData;
    }

    @Override
    public VersionedData versionedRemove(ByteBuffer key) {
        byte[] keyArr = key.array();
        byte[] fullKey = new byte[2 + ByteUtils.SIZE_OF_LONG + keyArr.length];
        ByteBuffer fullKeyBuffer = ByteBuffer.wrap(fullKey);
        fullKeyBuffer.put(KEY_VERSIONED_DATA_PREFIX);
        fullKeyBuffer.putLong(versionNo.incrementAndGet());
        fullKeyBuffer.put(REMOVAL_MARKER);
        fullKeyBuffer.put(keyArr);
        dbObj.put(fullKeyBuffer.array(), DUMMY_ENTRY);

        VersionedData vData = new VersionedData(versionNo.get(), false, key);
        return vData;
    }

    @Override
    public Iterator<VersionedData> getVersionedData() {
        final byte[] seekKey = new byte[1];
        ByteBuffer bb = ByteBuffer.wrap(seekKey);
        bb.put(KEY_VERSIONED_DATA_PREFIX);
        return getVersionedData(bb.array(), bb.array());
    }

    @Override
    public Iterator<VersionedData> getVersionedData(long versionNo) {
        final byte[] seekKey = new byte[1 + ByteUtils.SIZE_OF_LONG];
        final byte[] keyPrefix = new byte[1];

        ByteBuffer keyPrefixBB = ByteBuffer.wrap(keyPrefix);
        keyPrefixBB.put(KEY_VERSIONED_DATA_PREFIX);

        ByteBuffer seekKeyBB = ByteBuffer.wrap(seekKey);
        seekKeyBB.put(KEY_VERSIONED_DATA_PREFIX);
        seekKeyBB.putLong(versionNo);

        return getVersionedData(seekKeyBB.array(), keyPrefixBB.array());
    }

    private VersionedData getVersionedData(final byte[] rowKey) {
        int offset = 0;
        byte versionMarker = rowKey[offset];
        if(versionMarker != KEY_VERSIONED_DATA_PREFIX)
            return null;
        offset++;
        long versionNo = ByteUtils.readLong(rowKey, offset);
        offset += ByteUtils.SIZE_OF_LONG;
        byte dataMarker = rowKey[offset];
        boolean addedOrRemoved = (dataMarker == ADDITION_MARKER) ? true : false;
        offset++;
        int length = rowKey.length - offset;
        byte[] dataKey = new byte[length];
        System.arraycopy(rowKey, offset, dataKey, 0, length);

        return new VersionedData(versionNo, addedOrRemoved, ByteBuffer.wrap(dataKey));
    }

    private Iterator<VersionedData> getVersionedData(final byte[] startKey, final byte[] keyPrefix) {
        final DBIterator iterator = dbObj.iterator();
        iterator.seek(startKey);

        return new Iterator<VersionedData>() {

            private Queue<VersionedData> queue = new ArrayDeque<VersionedData>(1);

            @Override
            public boolean hasNext() {
                if(queue.size() == 0)
                    load();
                return queue.size() > 0;
            }

            @Override
            public VersionedData next() {
                if(queue.size() == 0)
                    throw new NoSuchElementException();
                return queue.remove();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void load() {
                if(iterator.hasNext()) {
                    if(ByteUtils.compare(keyPrefix,
                                         iterator.peekNext().getKey(),
                                         0,
                                         keyPrefix.length) != 0)
                        return;
                    byte[] rowKey = iterator.peekNext().getKey();
                    byte[] rowValue = iterator.peekNext().getValue();
                    VersionedData vData = getVersionedData(rowKey);
                    if(vData.isAddedOrRemoved())
                        vData.setValue(rowValue);
                    queue.add(vData);
                    iterator.next();
                }
            }

        };
    }

    @Override
    public VersionedData fetchVersionedData(long versionNo) {
        Iterator<VersionedData> itr = getVersionedData(versionNo);
        if(itr.hasNext())
            return itr.next();
        return null;
    }

    @Override
    public long getLatestVersionNo() {
        long latestVersionNo = loadVersionNo();
        return latestVersionNo;
    }
}
