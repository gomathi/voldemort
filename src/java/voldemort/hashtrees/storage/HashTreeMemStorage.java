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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;
import voldemort.hashtrees.thrift.generated.VersionedData;

import com.google.common.collect.PeekingIterator;

/**
 * In memory implementation of {@link HashTreeStorage} used only for unit
 * testing.
 * 
 */
@Threadsafe
public class HashTreeMemStorage implements HashTreeStorage {

    private final int noOfSegDataBlocks;
    private final ConcurrentMap<Integer, IndHashTreeMemStorage> treeIdAndIndHashTree = new ConcurrentHashMap<Integer, IndHashTreeMemStorage>();
    private final ConcurrentSkipListMap<Long, VersionedData> versionedData = new ConcurrentSkipListMap<Long, VersionedData>();
    private final AtomicLong versionNo = new AtomicLong();

    public HashTreeMemStorage(int noOfSegDataBlocks) {
        this.noOfSegDataBlocks = noOfSegDataBlocks;
    }

    private IndHashTreeMemStorage getIndHTree(int treeId) {
        if(!treeIdAndIndHashTree.containsKey(treeId))
            treeIdAndIndHashTree.putIfAbsent(treeId, new IndHashTreeMemStorage(noOfSegDataBlocks));
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
    public PeekingIterator<VersionedData> getVersionedData() {
        final Iterator<VersionedData> localItr = versionedData.values().iterator();
        return new VersionedDataPeekingIterator(localItr);
    }

    @Override
    public PeekingIterator<VersionedData> getVersionedData(long versionNo) {
        final Iterator<VersionedData> localItr = versionedData.tailMap(versionNo)
                                                              .values()
                                                              .iterator();
        return new VersionedDataPeekingIterator(localItr);
    }

    @Override
    public VersionedData versionedPut(ByteBuffer key, ByteBuffer value) {
        VersionedData vData = new VersionedData(versionNo.incrementAndGet(), true, key);
        vData.setValue(value);
        versionedData.put(vData.getVersionNo(), vData);
        return vData;
    }

    @Override
    public VersionedData versionedRemove(ByteBuffer key) {
        VersionedData vData = new VersionedData(versionNo.incrementAndGet(), false, key);
        versionedData.put(vData.getVersionNo(), vData);
        return vData;
    }

    @Override
    public VersionedData fetchVersionedData(long versionNo) {
        return versionedData.get(versionNo);
    }

    @Override
    public void deleteAllVersionedData() {
        versionedData.clear();
    }

    @Override
    public long getLatestVersionNo() {
        return versionNo.get();
    }

    private static class VersionedDataPeekingIterator implements PeekingIterator<VersionedData> {

        private final Queue<VersionedData> queue = new ArrayDeque<VersionedData>(1);
        private final Iterator<VersionedData> itr;

        public VersionedDataPeekingIterator(Iterator<VersionedData> itr) {
            this.itr = itr;
        }

        private void loadData() {
            if(itr.hasNext())
                queue.add(itr.next());
        }

        @Override
        public boolean hasNext() {
            if(queue.size() == 0)
                loadData();
            return queue.size() > 0;
        }

        @Override
        public VersionedData peek() {
            if(!hasNext())
                throw new NoSuchElementException();
            return queue.peek();
        }

        @Override
        public VersionedData next() {
            if(!hasNext())
                throw new NoSuchElementException();
            return queue.peek();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}