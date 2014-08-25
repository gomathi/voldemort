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
package voldemort.hashtrees.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.hashtrees.synch.BGStoppableTask;

/**
 * An abstract class which queues elements, and which can be dequeued while
 * running as a background thread. This can be used to enable non blocking
 * versions of operations.
 * 
 * @param <T>
 */
public abstract class NonBlockingDataQueueService<T> extends BGStoppableTask {

    private static final Logger LOG = Logger.getLogger(NonBlockingDataQueueService.class);
    private final T stopMarker;
    private final BlockingQueue<T> que;

    public NonBlockingDataQueueService(T stopMarker, int queueSize) {
        this.stopMarker = stopMarker;
        que = new ArrayBlockingQueue<T>(queueSize);
    }

    public void enque(T data) {
        if(hasStopRequested() && data != stopMarker) {
            throw new IllegalStateException("Shut down is initiated. Unable to store the data.");
        }
        boolean status = que.offer(data);
        if(!status)
            LOG.warn("Segement data queue is full. Unable to add data to the queue.");
    }

    @Override
    public synchronized void stop(final CountDownLatch shutDownLatch) {
        super.stop(shutDownLatch);
        enque(stopMarker);
    }

    @Override
    public void run() {
        if(enableRunningStatus()) {
            for(;;) {
                try {
                    T data = que.take();
                    if(data == stopMarker)
                        continue;
                    handleElement(data);
                } catch(InterruptedException e) {
                    LOG.error("Interrupted while waiting for removing an element from the queue. Exiting");
                    return;
                } catch(Exception e) {
                    LOG.error("Exception occurred while adding the element.", e);
                } finally {
                    if(hasStopRequested() && que.isEmpty()) {
                        disableRunningStatus();
                        return;
                    }
                }
            }
        }
    }

    public abstract void handleElement(T data);

}
