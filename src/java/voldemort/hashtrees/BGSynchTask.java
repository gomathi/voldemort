package voldemort.hashtrees;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
    private final ConcurrentSkipListSet<HostNameAndHashTreeId> hostNameAndTreeIdMap = new ConcurrentSkipListSet<BGSynchTask.HostNameAndHashTreeId>();
    private final HashTree localTree;

    private static class HostNameAndHashTreeId implements Comparator<HostNameAndHashTreeId> {

        private final String hostName;
        private final int treeId;

        public HostNameAndHashTreeId(String hostName, int treeId) {
            this.hostName = hostName;
            this.treeId = treeId;
        }

        @Override
        public int compare(HostNameAndHashTreeId o1, HostNameAndHashTreeId o2) {
            int compRes = o1.hostName.compareTo(o2.hostName);
            if(compRes != 0)
                return compRes;
            compRes = o1.treeId - o2.treeId;
            return compRes;
        }
    }

    public BGSynchTask(final CountDownLatch shutdownLatch, final HashTree localTree) {
        super(shutdownLatch);
        this.localTree = localTree;
    }

    private HashTree getHashTree(String hostName) {
        if(!hostNameAndRemoteHTrees.containsKey(hostName)) {
            hostNameAndRemoteHTrees.putIfAbsent(hostName, null);
        }
        return hostNameAndRemoteHTrees.get(hostName);
    }

    public void add(final String hostName, int treeId) {
        HostNameAndHashTreeId value = new HostNameAndHashTreeId(hostName, treeId);
        hostNameAndTreeIdMap.add(value);
        logger.debug("Host " + hostName + " and treeId :" + treeId
                     + " has been added from sync list.");
    }

    public void remove(final String hostName, int treeId) {
        hostNameAndTreeIdMap.remove(new HostNameAndHashTreeId(hostName, treeId));
        logger.debug("Host " + hostName + " and treeId :" + treeId
                     + " has been removed from sync list.");
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                logger.info("Synching remote hash trees.");
                for(HostNameAndHashTreeId syncHostAndTreeId: hostNameAndTreeIdMap) {
                    HashTree remoteTree = getHashTree(syncHostAndTreeId.hostName);
                    localTree.synch(syncHostAndTreeId.treeId, remoteTree);
                }
                logger.info("Synching remote hash trees. - Done");
            } finally {
                disableRunningStatus();
            }
        }
    }

}