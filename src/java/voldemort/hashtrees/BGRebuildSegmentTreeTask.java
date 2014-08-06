package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

/**
 * This updates just the segment hashes on the tree.
 * 
 */
public class BGRebuildSegmentTreeTask extends BGStoppableTask {

    private static final Logger logger = Logger.getLogger(BGRebuildSegmentTreeTask.class);
    private final HashTree hTree;

    public BGRebuildSegmentTreeTask(final HashTree hTree) {
        this.hTree = hTree;
    }

    public BGRebuildSegmentTreeTask(final HashTree hTree, final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
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
