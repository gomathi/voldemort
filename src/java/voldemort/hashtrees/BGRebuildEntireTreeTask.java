package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * This reads all the keys from storage, and rebuilds the complete HTree.
 */

@Threadsafe
public class BGRebuildEntireTreeTask extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(BGRebuildEntireTreeTask.class);

    public BGRebuildEntireTreeTask(final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
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

        long endTime = System.currentTimeMillis();
        logger.info("Total time took for rebuilding htree (in ms) : " + (endTime - startTime));
        logger.info("Rebuilding HTree - Done");
    }
}
