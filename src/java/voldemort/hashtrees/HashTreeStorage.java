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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

/**
 * Defines storage interface to be used by higher level HTree.
 * 
 */
public interface HashTreeStorage {

    /**
     * A segment data is the value inside a segment block.
     * 
     * @param treeId
     * @param segId
     * @param key
     * @param digest
     */
    void putSegmentData(int treeId, int segId, ByteBuffer key, ByteBuffer digest);

    /**
     * Deletes the given segement data from the block.
     * 
     * @param treeId
     * @param segId
     * @param key
     */
    void deleteSegmentData(int treeId, int segId, ByteBuffer key);

    /**
     * Returns the SegmentData for the given key if available, otherwise returns
     * null.
     * 
     * @param treeId
     * @param segId
     * @param key
     * @return
     */
    SegmentData getSegmentData(int treeId, int segId, ByteBuffer key);

    /**
     * Given a segment id, returns the list of all segment data in the
     * individual segment block.
     * 
     * @param treeId
     * @param segId
     * @return
     */
    List<SegmentData> getSegment(int treeId, int segId);

    /**
     * Segment hash is the hash of all data inside a segment block. A segment
     * hash is stored on a tree node.
     * 
     * @param treeId
     * @param nodeId, identifier of the node in the hash tree.
     * @param digest
     */
    void putSegmentHash(int treeId, int nodeId, ByteBuffer digest);

    /**
     * 
     * @param treeId
     * @param nodeId
     * @return
     */
    SegmentHash getSegmentHash(int treeId, int nodeId);

    /**
     * Returns the data inside the nodes of the hash tree. If the node id is not
     * present in the hash tree, the entry will be missing in the result.
     * 
     * @param treeId
     * @param nodeIds, internal tree node ids.
     * @return
     */
    List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds);

    /**
     * Marks a segment as a dirty.
     * 
     * @param treeId
     * @param segId
     */
    void setDirtySegment(int treeId, int segId);

    /**
     * Returns all the ids of the dirty segments. Dirty markers are reset while
     * returning the result. This operation is atomic.
     * 
     * @param treeId
     * @return
     */
    List<Integer> clearAndGetDirtySegments(int treeId);

    /**
     * Unsets all dirty segments. Used during the tree rebuild.
     * 
     * @param treeId
     */
    void clearAllSegments(int treeId);

    /**
     * Deletes the segment hashes, and segment data for the given treeId.
     * 
     * @param treeId
     */
    void deleteTree(int treeId);

    /**
     * Stores the timestamp at which the complete HashTree was rebuilt. This
     * method updates the value in storage only if the given value is higher
     * than the existing timestamp, otherwise a noop.
     * 
     * @param timestamp
     */
    void setLastFullyTreeBuiltTimestamp(int treeId, long timestamp);

    /**
     * Returns the timestamp at which the complete HashTree was rebuilt.
     * 
     * @return
     */
    long getLastFullyTreeReBuiltTimestamp(int treeId);

    /**
     * Sets the timestamp at which segment hashes were updated, not the entire
     * tree.
     * 
     * @param treeId
     * @param timestamp
     */
    void setLastHashTreeUpdatedTimestamp(int treeId, long timestamp);

    /**
     * Retrieves the timestamp at which segment hashes were updated.
     * 
     * @param treeId
     * @return
     */
    long getLastHashTreeUpdatedTimestamp(int treeId);
}
