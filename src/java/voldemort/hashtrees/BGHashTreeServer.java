package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface.Iface;

/**
 * This class launches a server in order for other nodes to communicate and
 * update the HashTree on this node.
 * 
 */
public class BGHashTreeServer extends BGStoppableTask {

    private final TServer server;

    public BGHashTreeServer(final CountDownLatch shutdownLatch,
                            final int serverPortNo,
                            final HashTree localHashTree) throws TTransportException {
        super(shutdownLatch);
        this.server = createServer(serverPortNo, localHashTree);
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            try {
                startServer();
            } finally {
                disableRunningStatus();
            }
        }
    }

    @Override
    public void stop() {
        stopServer();
        super.stop();
    }

    private static TServer createServer(int serverPortNo, HashTree hashTree)
            throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(serverPortNo);
        HashTreeSyncInterface.Processor<Iface> processor = new HashTreeSyncInterface.Processor<HashTreeSyncInterface.Iface>(hashTree);
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        return server;
    }

    private void startServer() {
        server.serve();
    }

    private void stopServer() {
        server.stop();
    }
}
