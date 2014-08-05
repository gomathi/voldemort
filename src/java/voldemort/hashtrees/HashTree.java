package voldemort.hashtrees;

import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import voldemort.hashtrees.thrift.HashTreeSyncInterface;

/**
 * Defines Hash tree methods. Hash tree provides a way for nodes to synch up
 * quickly by exchanging very little information.
 * 
 */
public interface HashTree extends HashTreeSyncInterface.Iface {

    /**
     * Adds the key, and digest of value to the segment block in HashTree.
     * 
     * @param key
     * @param value
     */
    void hPut(ByteBuffer key, ByteBuffer value);

    /**
     * Deletes the key from the hash tree.
     * 
     * @param key
     */
    void hRemove(ByteBuffer key);

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
    boolean synch(int treeId, HashTreeSyncInterface.Iface remoteTree) throws TException;

    /**
     * Implementation is expected to run a background task at regular intervals
     * to update remote hash trees. This function adds a remote tree to synch
     * list.
     * 
     * @param hostName
     * @param treeId
     * 
     */
    void addTreeToSyncList(String hostName, int treeId);

    /**
     * Removes the remoteTree from sync list.
     * 
     * @param remoteTree
     * @param treeId
     */
    void removeTreeFromSyncList(String hostName, int treeId);

    /**
     * Hash tree implementations do not update the segment hashes tree on every
     * key change. Rather tree is rebuilt at regular intervals. This function
     * provides an option to make a force call to update the entire tree.
     * 
     * @param fullRebuild, indicates whether to rebuild all segments, or just
     *        the dirty segments.
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
}
