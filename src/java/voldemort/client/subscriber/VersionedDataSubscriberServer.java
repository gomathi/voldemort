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
package voldemort.client.subscriber;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.thrift.generated.VersionedData;
import voldemort.hashtrees.thrift.generated.VersionedDataListenerService;

public class VersionedDataSubscriberServer {

    private final static Logger LOG = Logger.getLogger(VersionedDataSubscriberServer.class);

    public static void launchVersionedDataSubscriberServer(int serverPortNo,
                                                           SubscriberCallbackImpl subCallbackObj)
            throws TTransportException {
        LOG.info("Starting a server to subscribe the changes on " + serverPortNo);
        TServerSocket serverTransport = new TServerSocket(serverPortNo);
        VersionedDataListenerService.Processor<voldemort.hashtrees.thrift.generated.VersionedDataListenerService.Iface> processor = new VersionedDataListenerService.Processor<VersionedDataListenerService.Iface>(new VersionedDataSubscriberServerImpl(subCallbackObj));
        final TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
        new Thread(new Runnable() {

            @Override
            public void run() {
                server.serve();
            }
        }).start();
        LOG.info("Started a server to subscribe the changes on " + serverPortNo);
    }

    private static class VersionedDataSubscriberServerImpl implements
            VersionedDataListenerService.Iface {

        private final SubscriberCallbackImpl subscriberCallbackObj;

        public VersionedDataSubscriberServerImpl(SubscriberCallbackImpl subscriberCallbackObj) {
            this.subscriberCallbackObj = subscriberCallbackObj;
        }

        @Override
        public void post(List<VersionedData> vDataList) throws TException {
            subscriberCallbackObj.onPost(vDataList);
        }

    }
}
