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
package voldemort.hashtrees.core;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.thrift.generated.SegmentData;
import voldemort.hashtrees.thrift.generated.SegmentHash;

/**
 * Defines Hash tree methods. Hash tree provides a way for nodes to synch up
 * quickly by exchanging very little information.
 * 
 */
public interface HashTree {

    /**
     * Adds the (key,value) pair to the original storage. Intended to be used
     * while synch operation.
     * 
     * @param keyValuePairs
     */
    public void sPut(Map<ByteBuffer, ByteBuffer> keyValuePairs) throws Exception;

    /**
     * Deletes the keys from the storage. While synching this function is used.
     * 
     * @param keys
     * 
     */
    public void sRemove(List<ByteBuffer> keys) throws Exception;

    /**
     * Hash tree internal nodes store the hash of their children nodes. Given a
     * set of internal node ids, this returns the hashes that are stored on the
     * internal node.
     * 
     * @param treeId
     * @param nodeIds, internal tree node ids.
     * @return
     * 
     */
    public List<SegmentHash> getSegmentHashes(int treeId, List<Integer> nodeIds) throws Exception;

    /**
     * Returns the segment hash that is stored on the tree.
     * 
     * @param treeId, hash tree id.
     * @param nodeId, node id
     * @return
     * 
     */
    public SegmentHash getSegmentHash(int treeId, int nodeId) throws Exception;

    /**
     * Hash tree data is stored on the leaf blocks. Given a segment id this
     * method is supposed to return (key,hash) pairs.
     * 
     * @param treeId
     * @param segId, id of the segment block.
     * @return
     * 
     */
    public List<SegmentData> getSegment(int treeId, int segId) throws Exception;

    /**
     * Returns the (key,digest) for the given key in the given segment.
     * 
     * 
     * @param treeId
     * @param segId
     * @param key
     */
    public SegmentData getSegmentData(int treeId, int segId, ByteBuffer key) throws Exception;

    /**
     * Deletes tree nodes from the hash tree, and the corresponding segments.
     * 
     * 
     * @param treeId
     * @param nodeIds
     */
    public void deleteTreeNodes(int treeId, List<Integer> nodeIds) throws Exception;

    /**
     * Adds the key, and digest of value to the segment block in HashTree.
     * 
     * @param key
     * @param value
     */
    void hPut(ByteBuffer key, ByteBuffer value) throws Exception;

    /**
     * Deletes the key from the hash tree.
     * 
     * @param key
     */
    void hRemove(ByteBuffer key) throws Exception;

    /**
     * Updates the other HTree based on the differences with local objects.
     * 
     * This function should be running on primary to synch with other replicas,
     * and not the other way.
     * 
     * @param remoteTree
     * @return, true indicates some modifications made to the remote tree, false
     *          means two trees were already in synch status.
     */
    boolean synch(int treeId, HashTree remoteTree) throws Exception;

    /**
     * Hash tree implementations do not update the segment hashes tree on every
     * key change. Rather tree is rebuilt at regular intervals. This function
     * provides an option to make a force call to update the entire tree.
     * 
     * @param fullRebuild, indicates whether to rebuild all segments, or just
     *        the dirty segments.
     */
    void rebuildHashTrees(boolean fullRebuild) throws Exception;

    /**
     * Updates segment hashes based on the dirty entries.
     * 
     * @param treeId
     * @param fullRebuild, false indicates only update the hash trees based on
     *        the dirty entries, true indicates complete rebuild of the tree
     *        irrespective of dirty markers.
     */
    void rebuildHashTree(int treeId, boolean fullRebuild) throws Exception;

    /**
     * Returns the timestamp at which the tree was fully rebuilt.
     * 
     * @param treeId
     * @return
     */
    long getLastFullyRebuiltTimeStamp(int treeId) throws Exception;

    /**
     * Enables non blocking puts and removes operations.
     * 
     */
    void enableNonblockingOperations();

    /**
     * Disable non blocking puts and removes operations. By default hashtree
     * runs with blocking operations on puts and removes.
     */
    void disableNonblockingOperations();

    /**
     * All versions of the data are stored in {@link HashTreeStorage}. This can
     * be subscribed later to know all the changes that are happened to the
     * system.
     * 
     */
    void enableStoringVersionedData();

    /**
     * Disables storing the versioned data.
     * 
     */
    void disableStoringVersionedData();

    /**
     * Stops all operations if there are any background jobs are running.
     */
    void stop();
}
