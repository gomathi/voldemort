package voldemort.hashtrees.tasks;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * This reads all the keys from storage, and rebuilds the complete HTree. This
 * task can be scheduled through the executor service. This class makes sure
 * only one rebuilding task can run at any time. This task can be safely shut
 * down.
 * 
 */

@Threadsafe
public class BGRebuildEntireTreeTask extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(BGRebuildEntireTreeTask.class);

    private final ExecutorService executors;

    public BGRebuildEntireTreeTask(final ExecutorService executors,
                                   final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
        this.executors = executors;
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {

            executors.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        rebuildHashTree();
                    } finally {
                        disableRunningStatus();
                    }
                }
            });
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
