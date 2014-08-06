package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

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

    private final static Logger logger = Logger.getLogger(BGRebuildEntireTreeTask.class);
    private final HashTree hashTree;

    public BGRebuildEntireTreeTask(final HashTree hashtree) {
        this.hashTree = hashtree;
    }

    public BGRebuildEntireTreeTask(final HashTree hashtree, final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
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
            logger.info("A task for rebuilding hash tree is already running or stop has been requested. Skipping the current task.");
    }

    private void rebuildHashTree() {
        logger.info("Rebuilding HTree");
        long startTime = System.currentTimeMillis();
        hashTree.updateHashTrees(true);
        long endTime = System.currentTimeMillis();
        logger.info("Total time took for rebuilding htree (in ms) : " + (endTime - startTime));
        logger.info("Rebuilding HTree - Done");
    }
}
