package voldemort.hashtrees;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;

/**
 * This task resynchs given set of remote htree objects. This task can be
 * scheduled through the executor service.
 * 
 */
@Threadsafe
public class BGSynchTask extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(BGSynchTask.class);
    private final ConcurrentMap<String, HashTreeSyncInterface.Iface> hostNameAndRemoteHTrees = new ConcurrentHashMap<String, HashTreeSyncInterface.Iface>();
    private final ConcurrentSkipListSet<HostNameAndHashTreeId> hostNameAndTreeIdMap = new ConcurrentSkipListSet<BGSynchTask.HostNameAndHashTreeId>();
    private final HashTree localTree;

    private static class HostNameAndHashTreeId implements Comparable<HostNameAndHashTreeId> {

        private final String hostName;
        private final int treeId;

        public HostNameAndHashTreeId(String hostName, int treeId) {
            this.hostName = hostName;
            this.treeId = treeId;
        }

        @Override
        public int compareTo(HostNameAndHashTreeId that) {
            int compRes = this.hostName.compareTo(that.hostName);
            if(compRes != 0)
                return compRes;
            compRes = this.treeId - that.treeId;
            return compRes;
        }

        @Override
        public String toString() {
            return hostName + ":" + treeId;
        }
    }

    public BGSynchTask(final CountDownLatch shutdownLatch, final HashTree localTree) {
        super(shutdownLatch);
        this.localTree = localTree;
    }

    private HashTreeSyncInterface.Iface getHashTreeClient(String hostName) throws TTransportException {
        if(!hostNameAndRemoteHTrees.containsKey(hostName)) {
            hostNameAndRemoteHTrees.putIfAbsent(hostName,
                                                HashTreeClient.getHashTreeClient(hostName));
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
                    try {
                        HashTreeSyncInterface.Iface remoteTree = getHashTreeClient(syncHostAndTreeId.hostName);
                        localTree.synch(syncHostAndTreeId.treeId, remoteTree);
                    } catch(TException e) {
                        // TODO Auto-generated catch block
                        logger.warn("Unable to synch remote hash tree server : "
                                    + syncHostAndTreeId, e);
                    }
                }
                logger.info("Synching remote hash trees. - Done");
            } finally {
                disableRunningStatus();
            }
        }
    }

}