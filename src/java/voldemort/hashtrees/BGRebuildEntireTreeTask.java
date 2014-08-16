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

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * This reads all the keys from storage, and rebuilds the complete HashTree.
 * 
 * It is possible that, key values are added/removed directly to/from the
 * storage. In that case, HashTree will be diverging from the original key
 * values. So it is necessary that we rebuild the HashTree at regular intervals
 * to avoid diverging.
 */

@Threadsafe
public class BGRebuildEntireTreeTask extends BGStoppableTask {

    private final static Logger LOG = Logger.getLogger(BGRebuildEntireTreeTask.class);
    private final HashTree hashTree;

    public BGRebuildEntireTreeTask(final HashTree hashtree) {
        this.hashTree = hashtree;
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                rebuildHashTree();
            } finally {
                disableRunningStatus();
            }
        } else
            LOG.info("A task for rebuilding hash tree is already running or stop has been requested. Skipping the current task.");
    }

    private void rebuildHashTree() {
        LOG.info("Rebuilding HTree");
        long startTime = System.currentTimeMillis();
        hashTree.updateHashTrees(true);
        long endTime = System.currentTimeMillis();
        LOG.info("Total time took for rebuilding htree (in ms) : " + (endTime - startTime));
        LOG.info("Rebuilding HTree - Done");
    }
}
