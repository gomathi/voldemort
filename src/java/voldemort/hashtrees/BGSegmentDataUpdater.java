package voldemort.hashtrees;

import java.nio.ByteBuffer;
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
 * {@link HashTreeImpl#hPut(ByteArray, ByteArray)} and
 * {@link HashTreeImpl#hRemove(ByteArray)} operation.
 * 
 */
@Threadsafe
public class BGSegmentDataUpdater extends BGStoppableTask {

    private static final Logger logger = Logger.getLogger(BGSegmentDataUpdater.class);
    private static final Pair<HTOperation, List<ByteBuffer>> STOP_MARKER = new Pair<HTOperation, List<ByteBuffer>>(HTOperation.STOP,
                                                                                                                   null);

    private final BlockingQueue<Pair<HTOperation, List<ByteBuffer>>> que = new ArrayBlockingQueue<Pair<HTOperation, List<ByteBuffer>>>(Integer.MAX_VALUE);
    private final HashTreeImpl hTreeImpl;

    public BGSegmentDataUpdater(final HashTreeImpl hTreeImpl) {
        this.hTreeImpl = hTreeImpl;
    }

    public BGSegmentDataUpdater(final HashTreeImpl hTreeImpl, final CountDownLatch shutdownLatch) {
        super(shutdownLatch);
        this.hTreeImpl = hTreeImpl;
    }

    public void enque(Pair<HTOperation, List<ByteBuffer>> data) {
        if(hasStopRequested() && data.getFirst() != HTOperation.STOP) {
            throw new IllegalStateException("Shut down is initiated. Unable to store the data.");
        }
        que.add(data);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        enque(STOP_MARKER);
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            for(;;) {
                try {
                    Pair<HTOperation, List<ByteBuffer>> pair = que.take();
                    switch(pair.getFirst()) {
                        case PUT:
                            hTreeImpl.putInternal(pair.getSecond().get(0), pair.getSecond().get(1));
                            break;
                        case REMOVE:
                            hTreeImpl.removeInternal(pair.getSecond().get(0));
                            break;
                        case STOP:
                            // no op
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