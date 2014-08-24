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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import voldemort.annotations.concurrency.Threadsafe;
import voldemort.hashtrees.HTOperation;
import voldemort.hashtrees.HashTreeImpl;
import voldemort.hashtrees.HashTreeManager;
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

    private static final Logger LOG = Logger.getLogger(BGSegmentDataUpdater.class);
    private static final int DEFAULT_QUE_SIZE = 10000;
    private static final Pair<HTOperation, List<ByteBuffer>> STOP_MARKER = new Pair<HTOperation, List<ByteBuffer>>(HTOperation.STOP,
                                                                                                                   null);

    private final BlockingQueue<Pair<HTOperation, List<ByteBuffer>>> que = new ArrayBlockingQueue<Pair<HTOperation, List<ByteBuffer>>>(DEFAULT_QUE_SIZE);
    private final HashTreeManager hTreeManager;

    public BGSegmentDataUpdater(final HashTreeManager hTreeManager) {
        this.hTreeManager = hTreeManager;
    }

    public void enque(Pair<HTOperation, List<ByteBuffer>> data) {
        if(hasStopRequested() && data.getFirst() != HTOperation.STOP) {
            throw new IllegalStateException("Shut down is initiated. Unable to store the data.");
        }
        boolean status = que.offer(data);
        if(!status)
            LOG.warn("Segement data queue is full. Unable to add data to the queue.");
    }

    @Override
    public synchronized void stop() {
        super.stop();
        enque(STOP_MARKER);
    }

    @Override
    public synchronized void stop(final CountDownLatch shutDownLatch) {
        super.stop(shutDownLatch);
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
                            hTreeManager.hPut(pair.getSecond().get(0), pair.getSecond().get(1));
                            break;
                        case REMOVE:
                            hTreeManager.hRemove(pair.getSecond().get(0));
                            break;
                        case STOP:
                            // no op
                            break;
                    }
                } catch(InterruptedException e) {
                    LOG.error("Interrupted while waiting for removing an element from the queue. Exiting");
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