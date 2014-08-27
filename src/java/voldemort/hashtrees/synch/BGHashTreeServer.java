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
package voldemort.hashtrees.synch;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.core.HashTree;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;
import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface.Iface;

/**
 * This class launches a server in order for other nodes to communicate and
 * update the HashTree on this node.
 * 
 */
@Threadsafe
public class BGHashTreeServer extends BGStoppableTask {

    private final static Logger LOG = Logger.getLogger(BGHashTreeServer.class);
    private volatile TServer server;
    private final HashTree localHashTree;
    private final HashTreeSyncManagerImpl htSynchMgr;
    private final int serverPortNo;
    private CountDownLatch initializedLatch;

    public BGHashTreeServer(final HashTree localHashTree,
                            final HashTreeSyncManagerImpl hashTreeMgr,
                            final int serverPortNo) {
        this(localHashTree, hashTreeMgr, serverPortNo, null);
    }

    public BGHashTreeServer(final HashTree hTree,
                            final HashTreeSyncManagerImpl htSynchMgr,
                            final int serverPortNo,
                            final CountDownLatch initializedLatch) {
        this.localHashTree = hTree;
        this.htSynchMgr = htSynchMgr;
        this.serverPortNo = serverPortNo;
        this.initializedLatch = initializedLatch;
    }

    @Override
    public void run() {
        if(server != null && server.isServing())
            return;
        try {
            startServer();
        } catch(TTransportException e) {
            LOG.error("Exception occurred while starting server.", e);
        }
    }

    @Override
    public void stop() {
        if(server.isServing())
            stopServer();
        super.stop();
    }

    private static TServer createServer(int serverPortNo, HashTreeServerImpl hashTreeServer)
            throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(serverPortNo);
        HashTreeSyncInterface.Processor<Iface> processor = new HashTreeSyncInterface.Processor<HashTreeSyncInterface.Iface>(hashTreeServer);
        TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        return server;
    }

    private void startServer() throws TTransportException {
        if(server == null)
            this.server = createServer(serverPortNo, new HashTreeServerImpl(localHashTree, htSynchMgr));
        if(initializedLatch != null)
            initializedLatch.countDown();
        server.serve();
        LOG.info("Hash tree server has started.");
    }

    private void stopServer() {
        server.stop();
        LOG.info("Hash tree server has stopped.");
    }

}
