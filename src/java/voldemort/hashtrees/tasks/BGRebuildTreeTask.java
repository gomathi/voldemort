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
package voldemort.hashtrees.tasks;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.core.HashTree;
import voldemort.hashtrees.core.HashTreeIdProvider;
import voldemort.hashtrees.synch.BGStoppableTask;
import voldemort.utils.Pair;

/**
 * This task can be configured to either rebuild only dirty segments or entire
 * tree. Entire tree rebuild involves reading all the keys from storage, and
 * rebuilding the complete HashTree.
 * 
 * Reason to rebuild entire tree: It is possible that, key values are
 * added/removed directly to/from the storage. In that case, HashTree will be
 * diverging from the original key values. So it is necessary that we rebuild
 * the HashTree at regular intervals to avoid diverging.
 */

@Threadsafe
public class BGRebuildTreeTask extends BGStoppableTask {

    private final static Logger LOG = Logger.getLogger(BGRebuildTreeTask.class);
    private final HashTree hashTree;
    private final HashTreeIdProvider treeIdProvider;
    private final boolean fullRebuild;
    private final ExecutorService fixedExecutors;
    private final long timeBoundForSingleTask;
    private final ConcurrentMap<Integer, Pair<Long, Future<Void>>> treeIdAndRebuildTask = new ConcurrentHashMap<Integer, Pair<Long, Future<Void>>>();
    private final String taskType;

    /**
     * 
     * @param hashtree
     * @param treeIdProvider
     * @param fixedExecutors
     * @param fullRebuild indicates whether a full rebuild or only rebuild of
     *        dirty segments.
     */
    public BGRebuildTreeTask(final HashTree hashtree,
                             final HashTreeIdProvider treeIdProvider,
                             final ExecutorService fixedExecutors,
                             final long timeBoundForSingleTask,
                             boolean fullRebuild) {
        this.hashTree = hashtree;
        this.treeIdProvider = treeIdProvider;
        this.fullRebuild = fullRebuild;
        this.fixedExecutors = fixedExecutors;
        this.timeBoundForSingleTask = timeBoundForSingleTask;
        this.taskType = (fullRebuild) ? "CompleTreeRebuild task" : "DirtySegmentsRebuild task";
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                rebuildHashTrees();
            } finally {
                disableRunningStatus();
            }
        } else
            LOG.info("A task for rebuilding hash tree is already running or stop has been requested. Skipping the current task.");
    }

    private void rebuildHashTrees() {
        List<Integer> treeIds = treeIdProvider.getAllTreeIds();
        for(int treeId: treeIds)
            rebuildHashTree(treeId);
    }

    private void rebuildHashTree(final int treeId) {

        if(treeIdAndRebuildTask.containsKey(treeId)) {
            Pair<Long, Future<Void>> timeAndRebuildTask = treeIdAndRebuildTask.get(treeId);
            if(timeAndRebuildTask != null && !timeAndRebuildTask.getSecond().isDone()) {
                long diff = System.currentTimeMillis() - timeAndRebuildTask.getFirst();
                if(diff > timeBoundForSingleTask)
                    LOG.warn(taskType + " for tree id " + treeId
                             + " is still running. Skipping the current task.");
                return;
            }
        }

        long submittedTime = System.currentTimeMillis();
        Future<Void> rebuildTask = fixedExecutors.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                LOG.info(taskType + " for treeid " + treeId + " started.");
                long startTime = System.currentTimeMillis();

                hashTree.rebuildHashTree(treeId, fullRebuild);

                long endTime = System.currentTimeMillis();
                LOG.info("Total time took for " + taskType + " for treeId " + treeId + ","
                         + (endTime - startTime));
                LOG.info(taskType + " for treeid " + treeId + " - done.");
                return null;
            }
        });

        Pair<Long, Future<Void>> timeAndRbeuildTask = new Pair<Long, Future<Void>>(submittedTime,
                                                                                   rebuildTask);
        treeIdAndRebuildTask.put(treeId, timeAndRbeuildTask);
    }
}
