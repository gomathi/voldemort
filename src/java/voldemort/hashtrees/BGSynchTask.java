package voldemort.hashtrees;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * This task resynchs given set of remote htree objects. This task can be
 * scheduled through the executor service.
 * 
 */
@Threadsafe
public class BGSynchTask extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(BGSynchTask.class);
    private final ConcurrentMap<String, HashTree> hostNameAndRemoteHTrees = new ConcurrentHashMap<String, HashTree>();

    public BGSynchTask(CountDownLatch shutdownLatch) {
        super(shutdownLatch);
    }

    public void add(String hostName, HashTree remoteHTree) {
        if(hostNameAndRemoteHTrees.putIfAbsent(hostName, remoteHTree) != null) {
            logger.debug(hostName + " is already present on the synch list. Skipping the host.");
            return;
        }
        logger.info(hostName + " is added to the synch list.");
    }

    public void remove(String hostName) {
        if(hostNameAndRemoteHTrees.remove(hostName) != null)
            logger.info(hostName + " is removed from synch list.");
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {

            } finally {
                disableRunningStatus();
            }
        }
    }

}