package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.Stoppable;

/**
 * A stoppable abstract class which can be scheduled through executors. This
 * abstract class makes sure only one task can run at any time. The
 * implementations are expected to provide code for {@link #run()} method. Also
 * the callers of stop method, can use the latch {@link #shutdownLatch} to wait
 * for the complete stop of this task.
 * 
 * Implementations need to get true value from {@link #enableRunningStatus()}
 * before doing the actual task in {@link #run()} method. Otherwise, should not
 * be doing any task. Also after finishing the task inside run method, need to
 * call {@link #disableRunningStatus()} method.
 * 
 */
@Threadsafe
public abstract class BGStoppableTask implements Runnable, Stoppable {

    // all variables are locked by instance of this object.
    private boolean running = false;
    private volatile boolean stopRequested = false;
    private final CountDownLatch shutdownLatch;

    public BGStoppableTask(CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
    }

    /**
     * If a rebuild task is already running or stop has been requested, this
     * will return false. Otherwise enables running status to be true.
     * 
     * @return
     */
    protected synchronized boolean enableRunningStatus() {
        if(stopRequested || running)
            return false;
        running = true;
        return true;
    }

    protected synchronized void disableRunningStatus() {
        if(!running)
            return;
        running = false;
        if(stopRequested)
            shutdownLatch.countDown();
    }

    protected boolean hasStopRequested() {
        return stopRequested;
    }

    @Override
    public synchronized void stop() {
        if(stopRequested)
            return;
        stopRequested = true;
        if(!running)
            shutdownLatch.countDown();
    }

    @Override
    public abstract void run();

}
