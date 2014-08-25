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
package voldemort.hashtrees.synch;

public interface HTSyncManager {

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
