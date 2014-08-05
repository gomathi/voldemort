package voldemort.hashtrees.thrift;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import voldemort.hashtrees.HashTreeConstants;

/**
 * A hashtree client which talks to a hashtree server in another node.
 * 
 */

public class HashTreeClient {

    public static HashTreeSyncInterface.Iface getHashTreeClient(String serverName)
            throws TTransportException {
        TTransport transport = new TSocket(serverName,
                                           HashTreeConstants.DEFAULT_HASH_TREE_SERVER_PORT_NO);
        transport.open();

        TProtocol protocol = new TBinaryProtocol(transport);

        return new HashTreeSyncInterface.Client(protocol);
    }
}
