package voldemort.hashtrees.tasks;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.hashtrees.HashTree;

/**
 * This updates just the segment hashes on the tree.
 * 
 */
public class BGRebuildSegmentTreeTask extends BGStoppableTask {

    private static final Logger logger = Logger.getLogger(BGRebuildSegmentTreeTask.class);
    private final HashTree hTree;

    public BGRebuildSegmentTreeTask(final HashTree hTree, final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
        this.hTree = hTree;
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                hTree.updateSegmentHashes();
            } finally {
                disableRunningStatus();
            }
        } else
            logger.debug("Another rebuild task is already running. Skipping this task.");
    }

}
