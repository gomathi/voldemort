package voldemort.hashtrees;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import voldemort.utils.ByteArray;

/**
 * Defines Hash tree methods. Hash tree provides a way for nodes to synch up
 * quickly by exchanging very little information.
 * 
 */
public interface HashTree {

    /**
     * Adds the key, and digest of value to the segment block in HashTree.
     * 
     * @param key
     * @param value
     */
    void hPut(ByteArray key, ByteArray value);

    /**
     * Deletes the key from the hash tree.
     * 
     * @param key
     */
    void hRemove(ByteArray key);

    /**
     * Adds the key,value pair to the storage, not the hash tree. This is used
     * while sync operation between two nodes.
     * 
     * @param key
     * @param value
     */
    void sPut(ByteArray key, ByteArray value);

    /**
     * Adds the (key,value) pair to the original storage. Intended to be used
     * while synch operation.
     * 
     * @param key
     * @param value
     */
    void sPut(Map<ByteArray, ByteArray> keyValuePairs);

    /**
     * Removes the key from storage.
     * 
     * @param key
     */
    void sRemove(ByteArray key);

    /**
     * Deletes the keys from the storage. While synching this function is used.
     * 
     * @param key
     */
    void sRemove(List<ByteArray> key);

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
    boolean synch(int treeId, HashTree remoteTree);

    /**
     * Implementation is expected to run a background task at regular intervals
     * to update remote hash trees. This function adds a remote tree to synch
     * list.
     * 
     * @param remoteTree
     */
    void addTreeToSyncList(String hostName, HashTree remoteTree);

    /**
     * Removes the remoteTree from sync list.
     * 
     * @param remoteTree
     */
    void removeTreeFromSyncList(String hostName);

    /**
     * Returns the segment hash that is stored on the tree.
     * 
     * @param treeId, hash tree id.
     * @param nodeId, node id
     * @return
     */
    SegmentHash getSegmentHash(int treeId, int nodeId);

    /**
     * Hash tree internal nodes store the hash of their children nodes. Given a
     * set of internal node ids, this returns the hashes that are stored on the
     * internal node.
     * 
     * @param nodeIds, internal tree node ids.
     * @return
     */
    List<SegmentHash> getSegmentHashes(int treeId, Collection<Integer> nodeIds);

    /**
     * Returns the (key,digest) for the given key in the given segment.
     * 
     * @param treeId, hash tree id
     * @param segId
     * @param key
     * @return
     */
    SegmentData getSegmentData(int treeId, int segId, ByteArray key);

    /**
     * Hash tree data is stored on the leaf blocks. Given a segment id this
     * method is supposed to return (key,hash) pairs.
     * 
     * @param segId, id of the segment block.
     * @return
     */
    List<SegmentData> getSegment(int treeId, int segId);

    /**
     * Hash tree implementations do not update the segment hashes tree on every
     * key change. Rather tree is rebuilt at regular intervals. This function
     * provides an option to make a force call to update the entire tree.
     */
    void updateHashTrees(boolean fullRebuild);

    /**
     * Updates segment hashes based on the dirty entries.
     * 
     * @param treeId
     * @param fullRebuild, false indicates only update the hash trees based on
     *        the dirty entries, true indicates complete rebuild of the tree
     *        irrespective of dirty markers.
     */
    void updateHashTree(int treeId, boolean fullRebuild);

    /**
     * Deletes tree nodes from the hash tree, and the corresponding segments.
     * 
     * @param treeId
     * @param nodeIds
     */
    void deleteTreeNodes(int treeId, Collection<Integer> nodeIds);
}
