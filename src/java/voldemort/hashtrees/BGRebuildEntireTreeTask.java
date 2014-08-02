package voldemort.hashtrees;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

/**
 * This reads all the keys from storage, and rebuilds the complete HTree.
 */

@Threadsafe
public class BGRebuildEntireTreeTask extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(BGRebuildEntireTreeTask.class);
    private final HashTree hashTree;
    private final HashTreeStorage htStorage;
    private final Storage storage;

    public BGRebuildEntireTreeTask(final HashTree hashtree,
                                   final HashTreeStorage htStorage,
                                   final Storage storage,
                                   final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
        this.hashTree = hashtree;
        this.htStorage = htStorage;
        this.storage = storage;
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
        Iterator<Pair<ByteArray, ByteArray>> iterator = storage.iterator();

        long endTime = System.currentTimeMillis();
        logger.info("Total time took for rebuilding htree (in ms) : " + (endTime - startTime));
        logger.info("Rebuilding HTree - Done");
    }
}
