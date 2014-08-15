/*
 * Copyright 2008-2014 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface.Iface;

/**
 * This class launches a server in order for other nodes to communicate and
 * update the HashTree on this node.
 * 
 */
@Threadsafe
public class BGHashTreeServer extends BGStoppableTask {

    private final static Logger logger = Logger.getLogger(BGHashTreeServer.class);
    private final TServer server;

    public BGHashTreeServer(final HashTree localHashTree, final int serverPortNo)
                                                                                 throws TTransportException {
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

    @Override
    public void stop(final CountDownLatch shutdownLatch) {
        stopServer();
        super.stop(shutdownLatch);
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
        logger.debug("Hash tree server has started.");
    }

    private void stopServer() {
        server.stop();
        logger.debug("Hash tree server has stopped.");
    }
}
