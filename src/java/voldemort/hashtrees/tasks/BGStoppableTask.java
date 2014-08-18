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
package voldemort.hashtrees.tasks;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.utils.Stoppable;

/**
 * A stoppable abstract class which can be scheduled through executors. This
 * abstract class makes sure only one task can run at any time. The
 * implementations are expected to provide code for {@link #run()} method. Also
 * the callers of stop method, can use the latch {@link #stopListenerLatch} to
 * wait for the complete stop of this task.
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
    private final ReentrantLock runLock = new ReentrantLock();
    private volatile CountDownLatch stopListenerLatch;
    private volatile boolean stopRequested = false;

    /**
     * If a task is already running or stop has been requested, this will return
     * false. Otherwise enables running status to be true.
     * 
     * @return
     */
    protected synchronized boolean enableRunningStatus() {
        if(stopRequested)
            return false;
        return runLock.tryLock();
    }

    protected synchronized void disableRunningStatus() {
        runLock.unlock();
        if(stopRequested && stopListenerLatch != null)
            stopListenerLatch.countDown();
    }

    protected boolean hasStopRequested() {
        return stopRequested;
    }

    @Override
    public synchronized void stop() {
        if(stopRequested)
            return;
        stopRequested = true;
        if(!runLock.isLocked() && stopListenerLatch != null)
            stopListenerLatch.countDown();
    }

    public synchronized void stop(final CountDownLatch stopListenerLatch) {
        this.stopListenerLatch = stopListenerLatch;
        stop();
    }

    @Override
    public abstract void run();

    /**
     * This is to reset to initialized state, once a task is stopped.
     */
    public synchronized void reset() {
        stopRequested = false;
        stopListenerLatch = null;
    }
}
