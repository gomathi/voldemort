package voldemort.hashtrees;

public interface HashTreeLocalSyncManager {

    /**
     * Sets the remote hostname and port no.
     * 
     * @param hostName
     * @param portNo is used while syncing with the remote hashtree.
     */
    void addTreeAndPortNoForSync(String hostName, int portNo);

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

}
