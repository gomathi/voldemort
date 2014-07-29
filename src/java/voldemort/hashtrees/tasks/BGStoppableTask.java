package voldemort.hashtrees.tasks;

import java.util.concurrent.CountDownLatch;

/**
 * A stoppable abstract class, and the implementations are expected to provide
 * code for {@link #run()} method. Also the callers of stop method, can use the
 * latch {@link #shutdownLatch} to wait for the complete stop of this task.
 * 
 * Implementations need to get true value from {@link #enableRunningStatus()}
 * before doing the actual task in {@link #run()} method. Otherwise, should not
 * be doing any task. Also after finishing the task inside run method, need to
 * call {@link #disableRunningStatus()} method.
 * 
 */
public abstract class BGStoppableTask implements Runnable, Stoppable {

    private boolean running = false;
    private boolean stopRequested = false;

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
        running = false;
        if(stopRequested)
            shutdownLatch.countDown();
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
