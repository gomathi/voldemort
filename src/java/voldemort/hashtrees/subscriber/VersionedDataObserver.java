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
import java.util.Observable;
import java.util.Observer;
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

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import voldemort.annotations.concurrency.NotThreadsafe;
import voldemort.hashtrees.core.HashTreeImpl;
import voldemort.hashtrees.core.TaskQueue;
import voldemort.hashtrees.storage.HashTreeStorage;
import voldemort.hashtrees.storage.VersionedDataStorage;
import voldemort.hashtrees.thrift.generated.VersionedData;
import voldemort.hashtrees.thrift.generated.VersionedDataListenerService;
import voldemort.hashtrees.thrift.generated.VersionedDataListenerService.Client;
import voldemort.utils.ByteUtils;
import voldemort.utils.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * An observer that listens to all the changes that are happening in the
 * {@link VersionedDataStorage} and updates all the subscribers with that
 * changes.
 * 
 */
public class VersionedDataObserver implements Observer {

    private final static Logger LOG = Logger.getLogger(VersionedDataObserver.class);
    private static final long CLIENT_BATCH_SIZE = 1024 * 8; // in bytes
    /**
     * This determines how much we can buffer the {@link VersionedData} in
     * memory.
     */
    private static final int DEFAULT_QUE_SIZE = 100;
    private static final VersionedData STOP_MARKER = new VersionedData();
    private static final List<VersionedData> STOP_MARKER_LIST = new ArrayList<VersionedData>(1);

    private final ExecutorService executors;
    private final HashTreeImpl hashTree;
    private final HashTreeStorage hashTreeStorage;
    private final ConcurrentMap<Pair<String, Integer>, VersionedDataListenerService.Client> listeners = new ConcurrentHashMap<Pair<String, Integer>, VersionedDataListenerService.Client>();
    private final VersionedDataAdder versionedDataAdder;
    // This queue is shared between dispatcher and subscriber updater.
    private final BlockingQueue<List<VersionedData>> batchedDataQueue;
    private final BatchedDataDispatcher<VersionedData> batchedDataDispatcher;
    private final SubscriberUpdater clientDataUpdater = new SubscriberUpdater();

    private volatile boolean started;
    private volatile boolean stopRequested = false;
    private volatile CountDownLatch stopRequestLatch;

    public VersionedDataObserver(HashTreeImpl hashTree,
                                 final HashTreeStorage hashTreeStorage,
                                 int noOfBGThreads) {
        this.hashTree = hashTree;
        this.hashTreeStorage = hashTreeStorage;
        this.executors = Executors.newFixedThreadPool(noOfBGThreads);
        this.versionedDataAdder = new VersionedDataAdder(hashTreeStorage.getLatestVersionNo() + 1);
        this.batchedDataQueue = new ArrayBlockingQueue<List<VersionedData>>(DEFAULT_QUE_SIZE);
        this.batchedDataDispatcher = new BatchedDataDispatcher<VersionedData>(CLIENT_BATCH_SIZE,
                                                                              batchedDataQueue,
                                                                              versionedDataAdder,
                                                                              new VersionedDataHeapSizeCalculator());
    }

    public synchronized void start() {
        if(!started) {
            hashTree.addObserver(this);
            new Thread(batchedDataDispatcher).start();
            new Thread(clientDataUpdater).start();
            started = true;
        }
    }

    public synchronized void stop() {
        enque(STOP_MARKER);
        stopRequested = true;
    }

    public synchronized void stop(final CountDownLatch stopRequestLatch) {
        stop();
        if(!stopRequested)
            this.stopRequestLatch = stopRequestLatch;
    }

    @Override
    public void update(Observable o, Object arg) {
        if(o == hashTree) {
            enque((VersionedData) arg);
        }
    }

    public void enque(VersionedData vData) {
        if(stopRequested && vData != STOP_MARKER)
            return;
        versionedDataAdder.readData();
    }

    public void addListenerHostNameAndPortNo(String hostName, int portNo)
            throws TTransportException {
        Pair<String, Integer> listenerHostAndPortNo = Pair.create(hostName, portNo);
        listeners.putIfAbsent(listenerHostAndPortNo, getClient(hostName, portNo));
        LOG.info(listenerHostAndPortNo + " has been added for sync from the server.");
    }

    public void removeListenerHostNameAndPortNo(String hostName, int portNo) {
        Pair<String, Integer> listenerHostAndPortNo = Pair.create(hostName, portNo);
        listeners.remove(listenerHostAndPortNo);
        LOG.info(listenerHostAndPortNo + " has been removed for sync from the server.");
    }

    private void postDataToClients(final List<VersionedData> vDataList) {
        if(vDataList == null || vDataList.isEmpty())
            return;
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
                                                                                         LOG.info("Syncing "
                                                                                                  + input);
                                                                                         try {
                                                                                             client.post(vDataList);
                                                                                             return true;
                                                                                         } catch(TException e) {
                                                                                             LOG.info("Exception occurred while syncing client with data",
                                                                                                      e);
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

    // This class is not threadsafe, should not be shared across multiple
    // threads.
    private class VersionedDataAdder implements Iterator<VersionedData> {

        private final BlockingQueue<VersionedData> queue = new ArrayBlockingQueue<VersionedData>(1);
        private final Iterator<VersionedData> itr;

        public VersionedDataAdder(long versionNo) {
            itr = hashTreeStorage.getVersionedData(versionNo);
        }

        public void readData() {
            loadData();
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public VersionedData next() {
            loadData();
            try {
                return queue.take();
            } catch(InterruptedException e) {
                LOG.warn("Interrupted while waiting for data to be available.");
                throw new RuntimeException(e);
            }
        }

        private void loadData() {
            if(queue.size() == 1)
                return;
            else if(itr.hasNext())
                queue.add(itr.next());
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private static interface HeapSizeCalculator<T> {

        /**
         * Returns approximate storage taken in heap memory by this particular
         * object.
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
    private class BatchedDataDispatcher<T> implements Runnable {

        private final BlockingQueue<List<T>> dispatchingQue;
        private final long batchSize;
        private final Iterator<T> dataItr;
        private final HeapSizeCalculator<T> heapSizeCalc;

        private volatile List<T> internalQue;
        private volatile long currHeapSizeOfQue;

        public BatchedDataDispatcher(long batchSize,
                                     final BlockingQueue<List<T>> dispatchingQue,
                                     final Iterator<T> dataItr,
                                     final HeapSizeCalculator<T> heapSizeCalc) {
            this.batchSize = batchSize;
            this.dispatchingQue = dispatchingQue;
            this.heapSizeCalc = heapSizeCalc;
            this.dataItr = dataItr;

            internalQue = new ArrayList<T>();
        }

        @Override
        public void run() {
            while(dataItr.hasNext()) {
                T data = dataItr.next();
                if(currHeapSizeOfQue + heapSizeCalc.heapSize(data) == batchSize) {
                    List<T> dispatchedData = new ArrayList<T>(internalQue);
                    try {
                        dispatchingQue.put(dispatchedData);
                    } catch(InterruptedException e) {
                        LOG.warn("Interrupted while adding element to the blocking queue", e);
                        return;
                    }
                    internalQue = new ArrayList<T>();
                }
                internalQue.add(data);
                currHeapSizeOfQue += heapSizeCalc.heapSize(data);
            }
        }
    }

    private class SubscriberUpdater implements Runnable {

        @Override
        public void run() {
            while(true) {
                try {
                    List<VersionedData> data = batchedDataQueue.take();
                    if(data != STOP_MARKER_LIST)
                        postDataToClients(data);
                } catch(InterruptedException e) {
                    LOG.warn("Interrupted while waiting to retrieve the data", e);
                    return;
                }
                if(stopRequested) {
                    LOG.warn("Not proceeding further as stop has been requested.");
                    return;
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
