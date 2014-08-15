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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.thrift.generated.HashTreeSyncInterface;

/**
 * A hashtree client which talks to a hashtree server in another node.
 * 
 */

public class HashTreeClientGenerator {

    public static HashTreeSyncInterface.Iface getHashTreeClient(String serverName, int portNo)
            throws TTransportException {
        TTransport transport = new TSocket(serverName, portNo);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new HashTreeSyncInterface.Client(protocol);
    }

    public static HashTreeSyncInterface.Iface getHashTreeClient(String serverName)
            throws TTransportException {
        return getHashTreeClient(serverName, HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO);
    }
}
