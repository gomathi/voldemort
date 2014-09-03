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
package voldemort.hashtrees.subscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.mortbay.log.Log;

import voldemort.annotations.concurrency.LockedBy;
import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.hashtrees.core.TaskQueue;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.VersionedDataStorage;
import voldemort.hashtrees.synch.BGStoppableTask;
import voldemort.hashtrees.thrift.generated.VersionedData;
import voldemort.hashtrees.thrift.generated.VersionedDataListenerService;
import voldemort.hashtrees.thrift.generated.VersionedDataListenerService.Client;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.PeekingIterator;

/**
 * An observer that listens to all the changes that are happening
 * <em>currently</em> in the {@link VersionedDataStorage} and updates all the
 * subscribers with that changes.
 * 
 */
public class VersionedDataObserver {

    private final static Logger LOG = Logger.getLogger(VersionedDataObserver.class);
    private static final long DATA_ADDER_INTERVAL = 30 * 1000; // in ms, 30
                                                               // seconds
    private static final long CLIENT_BATCH_SIZE = 1024 * 8; // in bytes
    /**
     * This determines how much we can buffer the {@link VersionedData} in
     * memory.
     */
    private static final int DEFAULT_QUE_SIZE = 100;
    private static final VersionedData NO_MORE_DATA_AVA_MARKER = new VersionedData();
    private static final List<VersionedData> STOP_MARKER_LIST = new ArrayList<VersionedData>(1);
    private static final VersionedData STOP_MARKER = new VersionedData();

    private final ExecutorService executors;
    private final ScheduledExecutorService scheduledExecutors;
    private final HashTreeStorage hashTreeStorage;
    private final ConcurrentMap<Pair<String, Integer>, VersionedDataListenerService.Client> listeners = new ConcurrentHashMap<Pair<String, Integer>, VersionedDataListenerService.Client>();
    private final ConcurrentMap<Pair<String, Integer>, List<VersionedData>> unsyncedData = new ConcurrentHashMap<Pair<String, Integer>, List<VersionedData>>();
    private final VersionedDataAdder versionedDataAdder;
    // This queue is shared between dispatcher and updater.
    private final BlockingQueue<List<VersionedData>> batchedDataQueue;
    private final BatchedDataDispatcher<VersionedData> batchedDataDispatcher;
    private final SubscriberUpdater clientDataUpdater = new SubscriberUpdater();

    @LockedBy("this")
    private volatile boolean started;
    private volatile long lastSyncedVersionNo;

    public VersionedDataObserver(final HashTreeStorage hashTreeStorage, int noOfBGThreads) {
        this.hashTreeStorage = hashTreeStorage;
        this.executors = Executors.newFixedThreadPool(noOfBGThreads);
        this.scheduledExecutors = Executors.newScheduledThreadPool(1);
        this.versionedDataAdder = new VersionedDataAdder(hashTreeStorage.getLatestVersionNo());
        this.batchedDataQueue = new ArrayBlockingQueue<List<VersionedData>>(DEFAULT_QUE_SIZE);
        this.batchedDataDispatcher = new BatchedDataDispatcher<VersionedData>(CLIENT_BATCH_SIZE,
                                                                              batchedDataQueue,
                                                                              versionedDataAdder,
                                                                              new VersionedDataHeapSizeCalculator(),
                                                                              NO_MORE_DATA_AVA_MARKER,
                                                                              STOP_MARKER);
    }

    public synchronized void start() {
        if(!started) {
            new Thread(batchedDataDispatcher).start();
            new Thread(clientDataUpdater).start();
            scheduledExecutors.scheduleWithFixedDelay(versionedDataAdder,
                                                      0,
                                                      DATA_ADDER_INTERVAL,
                                                      TimeUnit.MILLISECONDS);
            started = true;
            LOG.info("Started background threads.");
        }
    }

    public synchronized void stop() {
        if(started) {
            CountDownLatch stopLatch = new CountDownLatch(2);
            batchedDataDispatcher.stop(stopLatch);
            clientDataUpdater.stop(stopLatch);
            try {
                stopLatch.await();
            } catch(InterruptedException e) {
                LOG.warn("Interrupted while stopping background operations.", e);
                return;
            }
            scheduledExecutors.shutdownNow();
            started = false;
            LOG.info("Stopped all the background threads.");
        }
    }

    /**
     * Adds subscriber hostname and portno to the subscribers list.
     * 
     * @param hostName
     * @param portNo
     * @return a versionNo that can be used by the clients of this API from
     *         which version this publisher will send out the changes to the
     *         subscriber.
     * @throws TTransportException
     */
    public long addListenerHostNameAndPortNo(String hostName, int portNo)
            throws TTransportException {
        Pair<String, Integer> listenerHostAndPortNo = Pair.create(hostName, portNo);
        listeners.putIfAbsent(listenerHostAndPortNo, getClient(hostName, portNo));
        LOG.info(listenerHostAndPortNo + " has been added for sync from the server.");
        return lastSyncedVersionNo;
    }

    public void removeListenerHostNameAndPortNo(String hostName, int portNo) {
        Pair<String, Integer> listenerHostAndPortNo = Pair.create(hostName, portNo);
        listeners.remove(listenerHostAndPortNo);
        unsyncedData.remove(listenerHostAndPortNo);
        LOG.info(listenerHostAndPortNo + " has been removed for sync from the server.");
    }

    private void postDataToClients(final List<VersionedData> vDataList) {
        if(vDataList == null || vDataList.isEmpty())
            return;
        lastSyncedVersionNo = vDataList.get(vDataList.size() - 1).versionNo;
        Collection<Callable<Boolean>> tasks = Collections2.transform(listeners.keySet(),
                                                                     new Function<Pair<String, Integer>, Callable<Boolean>>() {

                                                                         @Override
                                                                         public Callable<Boolean> apply(final Pair<String, Integer> input) {
                                                                             return new Callable<Boolean>() {

                                                                                 @Override
                                                                                 public Boolean call()
                                                                                         throws Exception {
                                                                                     Client client = listeners.get(input);
                                                                                     if(client != null) {
                                                                                         List<VersionedData> dataToSend = unsyncedData.get(input);
                                                                                         if(dataToSend != null)
                                                                                             dataToSend.addAll(vDataList);
                                                                                         LOG.info("Syncing "
                                                                                                  + input);
                                                                                         try {
                                                                                             if(dataToSend == null)
                                                                                                 client.post(vDataList);
                                                                                             else
                                                                                                 client.post(dataToSend);
                                                                                             unsyncedData.remove(input);
                                                                                             return true;
                                                                                         } catch(TException e) {
                                                                                             LOG.info("Exception occurred while syncing client with data",
                                                                                                      e);
                                                                                             if(dataToSend == null)
                                                                                                 unsyncedData.put(input,
                                                                                                                  new ArrayList<VersionedData>(vDataList));
                                                                                             else
                                                                                                 unsyncedData.put(input,
                                                                                                                  dataToSend);
                                                                                             return false;
                                                                                         }
                                                                                     }
                                                                                     return true;
                                                                                 }
                                                                             };
                                                                         }

                                                                     });
        TaskQueue<Boolean> taskQueue = new TaskQueue<Boolean>(executors, tasks.iterator(), 10);
        int totSuccessful = 0;
        int totFailures = 0;
        while(taskQueue.hasNext()) {
            Future<Boolean> result = taskQueue.next();
            try {
                if(result.get())
                    totSuccessful++;
                else
                    totFailures++;
            } catch(InterruptedException e) {
                totFailures++;
            } catch(ExecutionException e) {
                totFailures++;
            }
        }

        LOG.info("Total no of successful synchs : " + totSuccessful);
        LOG.info("Total no of successful synchs : " + totFailures);
    }

    /**
     * This class is not threadsafe and should be shared with atmost two
     * threads. One is for calling {@link #loadData()}, another one is for
     * calling iterator functions {@link #hasNext()} and {@link #next()}.
     * 
     */
    private class VersionedDataAdder implements Iterator<VersionedData>, Runnable {

        private final BlockingQueue<VersionedData> queue = new ArrayBlockingQueue<VersionedData>(1);
        private volatile long lastReadVersionNo;

        public VersionedDataAdder(long lastReadVersionNo) {
            this.lastReadVersionNo = lastReadVersionNo;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public VersionedData next() {
            try {
                VersionedData vData = queue.poll(DATA_ADDER_INTERVAL, TimeUnit.MILLISECONDS);
                if(vData == null)
                    return NO_MORE_DATA_AVA_MARKER;
                return vData;
            } catch(InterruptedException e) {
                LOG.warn("Interrupted while waiting for data to be available.");
                throw new RuntimeException(e);
            }
        }

        private void loadData() throws InterruptedException {
            PeekingIterator<VersionedData> itr = hashTreeStorage.getVersionedData(lastReadVersionNo);
            if(itr.hasNext()) {
                if(itr.peek().getVersionNo() == lastReadVersionNo)
                    itr.next();
            }
            while(itr.hasNext()) {
                VersionedData vData = itr.next();
                queue.put(vData);
                lastReadVersionNo = vData.getVersionNo();
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void run() {
            try {
                loadData();
            } catch(InterruptedException e) {
                Log.warn("Interrupted while loading data.", e);
                return;
            }
        }

    }

    private static interface HeapSizeCalculator<T> {

        /**
         * Returns approximate storage taken in heap memory by this object.
         * 
         * @param data
         * @return
         */
        long heapSize(T data);
    }

    private static class VersionedDataHeapSizeCalculator implements
            HeapSizeCalculator<VersionedData> {

        @Override
        public long heapSize(VersionedData data) {
            long result = 0;
            result += ByteUtils.SIZE_OF_INT;
            result += ByteUtils.SIZE_OF_LONG;
            result += data.getKey().length;
            result += data.getValue().length;
            return result;
        }

    }

    @NotThreadsafe
    private static class BatchedDataDispatcher<T> extends BGStoppableTask {

        private final long batchSize;
        private final Iterator<T> dataItr;
        private final HeapSizeCalculator<T> heapSizeCalc;
        private final T noMoreDataAvaMarker;
        private final T stopMarker;
        private final BlockingQueue<List<T>> dispatchingQue;

        private volatile List<T> internalQue;
        private volatile long currHeapSizeOfQue;

        public BatchedDataDispatcher(long batchSize,
                                     final BlockingQueue<List<T>> dispatchingQue,
                                     final Iterator<T> dataItr,
                                     final HeapSizeCalculator<T> heapSizeCalc,
                                     final T noMoreDataAvaMarker,
                                     final T stopMarker) {
            this.batchSize = batchSize;
            this.dispatchingQue = dispatchingQue;
            this.heapSizeCalc = heapSizeCalc;
            this.dataItr = dataItr;
            this.noMoreDataAvaMarker = noMoreDataAvaMarker;
            this.stopMarker = stopMarker;

            internalQue = new ArrayList<T>();
        }

        @Override
        public void run() {
            if(enableRunningStatus()) {
                try {
                    while(dataItr.hasNext()) {
                        T data = dataItr.next();
                        if(data == stopMarker)
                            break;
                        if(currHeapSizeOfQue + heapSizeCalc.heapSize(data) >= batchSize
                           || data == noMoreDataAvaMarker) {
                            if(data != noMoreDataAvaMarker)
                                internalQue.add(data);
                            List<T> dispatchedData = new ArrayList<T>(internalQue);
                            try {
                                if(dispatchedData.size() > 0)
                                    dispatchingQue.put(dispatchedData);
                            } catch(InterruptedException e) {
                                LOG.warn("Interrupted while adding element to the blocking queue",
                                         e);
                                return;
                            }
                            internalQue = new ArrayList<T>();
                        }
                    }
                } finally {
                    disableRunningStatus();
                }
            }
        }
    }

    private class SubscriberUpdater extends BGStoppableTask {

        @Override
        public void run() {
            if(enableRunningStatus()) {
                while(true) {
                    try {
                        List<VersionedData> data = batchedDataQueue.take();
                        if(data != STOP_MARKER_LIST)
                            postDataToClients(data);
                    } catch(InterruptedException e) {
                        LOG.warn("Interrupted while waiting to retrieve the data", e);
                        return;
                    } finally {
                        disableRunningStatus();
                    }
                }
            }
        }

    }

    private static Client getClient(String hostName, int portNo) throws TTransportException {
        TTransport transport = new TSocket(hostName, portNo);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new VersionedDataListenerService.Client(protocol);
    }
}
