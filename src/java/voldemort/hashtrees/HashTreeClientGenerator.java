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
