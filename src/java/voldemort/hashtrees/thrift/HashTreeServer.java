package voldemort.hashtrees.thrift;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.BGStoppableTask;
import voldemort.hashtrees.HashTree;
import voldemort.hashtrees.thrift.HashTreeSyncInterface.Iface;

public class HashTreeServer extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(HashTreeServer.class);

    private final int serverPortNo;
    private final HashTree localHashTree;

    public HashTreeServer(final CountDownLatch shutdownLatch,
                          final int serverPortNo,
                          final HashTree localHashTree) {
        super(shutdownLatch);
        this.serverPortNo = serverPortNo;
        this.localHashTree = localHashTree;
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                startServer();
            } catch(TTransportException e) {
                // TODO Auto-generated catch block
                logger.error("Exception occurred while starting hash tree server.", e);
            } finally {
                disableRunningStatus();
            }
        }
    }

    private void startServer() throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(serverPortNo);
        HashTreeSyncInterface.Processor<Iface> processor = new HashTreeSyncInterface.Processor<HashTreeSyncInterface.Iface>(localHashTree);
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        server.serve();
    }
}
