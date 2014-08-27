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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.hashtrees.thrift.generated.VersionedData;

/**
 * In memory implementation of {@link HashTreeStorage} used only for unit
 * testing.
 * 
 */
@Threadsafe
public class HashTreeMemStorage implements HashTreeStorage {

    private final int noOfSegDataBlocks;
    private final ConcurrentMap<Integer, IndHashTreeMemStorage> treeIdAndIndHashTree = new ConcurrentHashMap<Integer, IndHashTreeMemStorage>();

    public HashTreeMemStorage(int noOfSegDataBlocks) {
        this.noOfSegDataBlocks = noOfSegDataBlocks;
    }

    private IndHashTreeMemStorage getIndHTree(int treeId) {
        if(!treeIdAndIndHashTree.containsKey(treeId))
            treeIdAndIndHashTree.putIfAbsent(treeId, new IndHashTreeMemStorage(treeId, noOfSegDataBlocks));
        return treeIdAndIndHashTree.get(treeId);
    }

    @Override
    public void putSegmentData(int treeId, int segId, ByteBuffer key, ByteBuffer digest) {
        getIndHTree(treeId).putSegmentData(segId, key, digest);
    }

    @Override
    public void deleteSegmentData(int treeId, int segId, ByteBuffer key) {
        getIndHTree(treeId).deleteSegmentData(segId, key);
    }

    @Override
    public List<SegmentData> getSegment(int treeId, int segId) {
        return getIndHTree(treeId).getSegment(segId);
    }

    @Override
    public void putSegmentHash(int treeId, int nodeId, ByteBuffer digest) {
        getIndHTree(treeId).putSegmentHash(nodeId, digest);
    }

    @Override
    public List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds) {
        return getIndHTree(treeId).getSegmentHashes(nodeIds);
    }

    @Override
    public void setDirtySegment(int treeId, int segId) {
        getIndHTree(treeId).setDirtySegment(segId);
    }

    @Override
    public List<Integer> clearAndGetDirtySegments(int treeId) {
        return getIndHTree(treeId).clearAndGetDirtySegments();
    }

    @Override
    public void deleteTree(int treeId) {
        treeIdAndIndHashTree.remove(treeId);
    }

    @Override
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) {
        return getIndHTree(treeId).getSegmentData(segId, key);
    }

    @Override
    public SegmentHash getSegmentHash(int treeId, int nodeId) {
        return getIndHTree(treeId).getSegmentHash(nodeId);
    }

    @Override
    public void clearAllSegments(int treeId) {
        getIndHTree(treeId).clearDirtySegments();
    }

    @Override
    public void setLastFullyTreeBuiltTimestamp(int treeId, long timestamp) {
        getIndHTree(treeId).setLastFullyRebuiltTimestamp(timestamp);
    }

    @Override
    public long getLastFullyTreeReBuiltTimestamp(int treeId) {
        return getIndHTree(treeId).getLastTreeFullyRebuiltTimestamp();
    }

    @Override
    public void setLastHashTreeUpdatedTimestamp(int treeId, long timestamp) {
        getIndHTree(treeId).setLastHashTreeUpdatedTimestamp(timestamp);
    }

    @Override
    public long getLastHashTreeUpdatedTimestamp(int treeId) {
        return getIndHTree(treeId).getLastHashTreeUpdatedTimestamp();
    }

    @Override
    public Iterator<VersionedData> getVersionedData(int treeId) {
        return getIndHTree(treeId).getVersionedData();
    }

    @Override
    public Iterator<VersionedData> getVersionedData(int treeId, long versionNo) {
        return getIndHTree(treeId).getVersionedData(versionNo);
    }

    @Override
    public VersionedData versionedPut(int treeId, ByteBuffer key, ByteBuffer value) {
        return getIndHTree(treeId).putVersionedDataAddition(key, value);
    }

    @Override
    public VersionedData versionedRemove(int treeId, ByteBuffer key) {
        return getIndHTree(treeId).putVersionedDataRemoval(key);
    }

}