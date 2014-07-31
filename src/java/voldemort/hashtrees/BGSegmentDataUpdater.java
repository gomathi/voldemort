package voldemort.hashtrees;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;

/**
 * A task to enable non blocking calls on all
 * {@link HashTreeImpl#put(ByteArray, ByteArray)} and
 * {@link HashTreeImpl#remove(ByteArray)} operation.
 * 
 */
@Threadsafe
public class BGSegmentDataUpdater extends BGStoppableTask {

    private static final Logger logger = Logger.getLogger(BGSegmentDataUpdater.class);

    private final BlockingQueue<Pair<HTOperation, List<ByteArray>>> que = new ArrayBlockingQueue<Pair<HTOperation, List<ByteArray>>>(Integer.MAX_VALUE);
    private final HashTreeImpl hTreeImpl;

    public BGSegmentDataUpdater(final CountDownLatch shutdownLatch, final HashTreeImpl hTreeImpl) {
        super(shutdownLatch);
        this.hTreeImpl = hTreeImpl;
    }

    public void enque(Pair<HTOperation, List<ByteArray>> data) {
        if(hasStopRequested()) {
            throw new IllegalStateException("Shut down is initiated. Unable to store the data.");
        }
        que.add(data);
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            for(;;) {
                try {
                    Pair<HTOperation, List<ByteArray>> pair = que.take();
                    switch(pair.getFirst()) {
                        case PUT:
                            hTreeImpl.putInternal(pair.getSecond().get(0), pair.getSecond().get(1));
                            break;
                        case REMOVE:
                            hTreeImpl.removeInternal(pair.getSecond().get(0));
                            break;
                    }
                } catch(InterruptedException e) {
                    // TODO Auto-generated catch block
                    logger.error("Interrupted while waiting for removing an element from the queue. Exiting");
                    return;
                } finally {
                    if(hasStopRequested() && que.isEmpty()) {
                        disableRunningStatus();
                        return;
                    }
                }
            }
        }
    }
}