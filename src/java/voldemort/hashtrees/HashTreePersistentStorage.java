package voldemort.hashtrees;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
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
 * [value] 3) SegmentHash, format is ['H'|treeId|nodeId] -> [value]
 * 
 * 
 */

public class HashTreePersistentStorage implements HashTreeStorage {

    private static final byte[] KEY_LAST_FULLY_TREE_BUILT_TS = "ltfbTs".getBytes();
    private static final byte[] KEY_LAST_TREE_BUILT_TS = "ltbTs".getBytes();
    private static final byte KEY_META_DATA_PREFIX = 'M';
    private static final byte KEY_SEG_HASH_PREFIX = 'H';
    private static final byte KEY_SEG_DATA_PREFIX = 'S';
    private static final Logger logger = Logger.getLogger(HashTreePersistentStorage.class);

    private final DB dbObj;
    private final int noOfSegDataBlocks;
    private final ConcurrentMap<Integer, AtomicBitSet> treeIdAndDirtySegmentMap = new ConcurrentHashMap<Integer, AtomicBitSet>();

    public HashTreePersistentStorage(String dbFileName, int noOfSegDataBlocks) throws IOException {
        this.dbObj = createDb(dbFileName);
        this.noOfSegDataBlocks = noOfSegDataBlocks;
    }

    private static DB createDb(String dbFileName) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        return new JniDBFactory().open(new File(dbFileName), options);
    }

    public void close() {
        try {
            dbObj.close();
        } catch(IOException e) {
            // TODO Auto-generated catch block
            logger.warn("Exception occurred while closing leveldb connection.");
        }
    }

    private AtomicBitSet getDirtySegmentsHolder(int treeId) {
        if(!treeIdAndDirtySegmentMap.containsKey(treeId))
            treeIdAndDirtySegmentMap.putIfAbsent(treeId, new AtomicBitSet(noOfSegDataBlocks));
        return treeIdAndDirtySegmentMap.get(treeId);
    }

    public static byte[] readSegmentDataKey(byte[] dbSegDataKey) {
        int length = dbSegDataKey.length;
        int from = (ByteUtils.SIZE_OF_INT * 2) + 1;
        byte[] key = ByteUtils.copy(dbSegDataKey, from, length);
        return key;
    }

    public static byte[] prepareSegmentHashKey(int treeId, int nodeId) {
        byte[] key = new byte[1 + ByteUtils.SIZE_OF_INT * 2];
        ByteBuffer bb = ByteBuffer.wrap(key);
        bb.put(KEY_SEG_HASH_PREFIX);
        bb.putInt(treeId);
        bb.putInt(nodeId);
        return key;
    }

    public static byte[] prepareSegmentDataKeyPrefix(int treeId, int segId) {
        byte[] byteKey = new byte[1 + ByteUtils.SIZE_OF_INT * 2];
        ByteBuffer bb = ByteBuffer.wrap(byteKey);
        bb.put(KEY_SEG_DATA_PREFIX);
        bb.putInt(treeId);
        bb.putInt(segId);
        return byteKey;
    }

    public static byte[] prepareSegmentDataKey(int treeId, int segId, ByteBuffer key) {
        byte[] byteKey = new byte[1 + ByteUtils.SIZE_OF_INT * 2 + (key.array().length)];
        ByteBuffer bb = ByteBuffer.wrap(byteKey);
        bb.put(KEY_SEG_DATA_PREFIX);
        bb.putInt(treeId);
        bb.putInt(segId);
        bb.put(key.array());
        return byteKey;
    }

    public static byte[] prepareMetaDataKey(int treeId, byte[] keyName) {
        byte[] byteKey = new byte[1 + ByteUtils.SIZE_OF_INT + keyName.length];
        ByteBuffer bb = ByteBuffer.wrap(byteKey);
        bb.put(KEY_META_DATA_PREFIX);
        bb.putInt(treeId);
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
    public void deleteTree(int treeId) {}

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

    private byte[] getFirstKeyWithPrefix(byte[] prefix) {
        DBIterator itr = dbObj.iterator();
        itr.seek(prefix);
        if(itr.hasNext())
            return itr.next().getKey();
        return null;
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        List<SegmentData> result = new ArrayList<SegmentData>();
        byte[] startKey = prepareSegmentDataKeyPrefix(treeId, segId);
        byte[] endKey = getFirstKeyWithPrefix(prepareSegmentDataKeyPrefix(treeId, segId + 1));
        boolean hasEndKey = (endKey == null) ? false : true;
        DBIterator iterator = dbObj.iterator();
        try {
            for(iterator.seek(startKey); iterator.hasNext(); iterator.next()) {
                ByteBuffer key = ByteBuffer.wrap(readSegmentDataKey(iterator.peekNext().getKey()));
                if(hasEndKey && ByteUtils.compare(endKey, iterator.peekNext().getKey()) == 0)
                    break;
                ByteBuffer digest = ByteBuffer.wrap(iterator.peekNext().getValue());
                result.add(new SegmentData(key, digest));

            }
        } finally {
            try {
                iterator.close();
            } catch(IOException e) {
                // TODO Auto-generated catch block
                logger.warn("Exception occurred while closing the DBIterator.", e);
            }
        }
        return result;
    }

}
