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

import org.apache.http.annotation.ThreadSafe;
import org.apache.log4j.Logger;

/**
 * This updates just the segment hashes on the tree.
 * 
 */
@ThreadSafe
public class BGRebuildSegmentTreeTask extends BGStoppableTask {

    private static final Logger logger = Logger.getLogger(BGRebuildSegmentTreeTask.class);
    private final HashTree hTree;

    public BGRebuildSegmentTreeTask(final HashTree hTree) {
        this.hTree = hTree;
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                rebuildSegmentTrees();
            } finally {
                disableRunningStatus();
            }
        } else
            logger.debug("Another rebuild task is already running. Skipping this task.");
    }

    private void rebuildSegmentTrees() {
        logger.info("Updating segment hashes : ");
        long startTime = System.currentTimeMillis();
        hTree.updateHashTrees(false);
        long endTime = System.currentTimeMillis();
        logger.info("Total time taken to update segment hashes : (in ms)" + (endTime - startTime));
    }
}
