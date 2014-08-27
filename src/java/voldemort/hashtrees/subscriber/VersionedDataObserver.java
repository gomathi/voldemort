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
