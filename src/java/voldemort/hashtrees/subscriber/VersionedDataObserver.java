package voldemort.hashtrees.subscriber;

import java.util.Observable;
import java.util.Observer;

import voldemort.hashtrees.core.HashTreeImpl;
import voldemort.hashtrees.core.NonBlockingDataQueueService;
import voldemort.hashtrees.thrift.generated.VersionedData;

public class VersionedDataObserver extends NonBlockingDataQueueService<VersionedData> implements
        Observer {

    private final HashTreeImpl hashTree;
    private volatile boolean started;

    private static final int DEFAULT_QUE_SIZE = 10000;
    private static final VersionedData STOP_MARKER = new VersionedData();

    public VersionedDataObserver(HashTreeImpl hashTree) {
        super(STOP_MARKER, DEFAULT_QUE_SIZE);
        this.hashTree = hashTree;
    }

    public void start() {
        if(!started) {
            hashTree.addObserver(this);
            started = true;
        }
    }

    @Override
    public void update(Observable o, Object arg) {
        if(o == hashTree) {
            enque((VersionedData) arg);
        }
    }

    @Override
    public void handleElement(VersionedData data) {

    }

}
