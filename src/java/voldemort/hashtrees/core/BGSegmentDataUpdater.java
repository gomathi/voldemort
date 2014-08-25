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

import java.nio.ByteBuffer;
import java.util.List;

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
public class BGSegmentDataUpdater extends
        NonBlockingDataQueueService<Pair<HTOperation, List<ByteBuffer>>> {

    private static final int DEFAULT_QUE_SIZE = 10000;
    private static final Pair<HTOperation, List<ByteBuffer>> STOP_MARKER = new Pair<HTOperation, List<ByteBuffer>>(HTOperation.PUT,
                                                                                                                   null);
    private final HashTreeImpl hTree;

    public BGSegmentDataUpdater(final HashTreeImpl hTree) {
        super(STOP_MARKER, DEFAULT_QUE_SIZE);
        this.hTree = hTree;
    }

    @Override
    public void handleElement(Pair<HTOperation, List<ByteBuffer>> pair) {
        switch(pair.getFirst()) {
            case PUT:
                hTree.hPutInternal(pair.getSecond().get(0), pair.getSecond().get(1));
                break;
            case REMOVE:
                hTree.hRemoveInternal(pair.getSecond().get(0));
                break;
        }
    }

}