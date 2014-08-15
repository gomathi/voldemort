/*
 * Copyright 2008-2009 LinkedIn, Inc
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
package voldemort.utils;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

import voldemort.annotations.concurrency.NotThreadsafe;

import com.google.common.collect.PeekingIterator;

@NotThreadsafe
public class CollectionPeekingIterator<T> implements PeekingIterator<T> {

    private final Queue<T> pQueue;
    private final Iterator<T> iItr;

    public CollectionPeekingIterator(Collection<T> collection) {
        pQueue = new ArrayDeque<T>(1);
        iItr = collection.iterator();
    }

    private void addElement() {
        if(iItr.hasNext())
            pQueue.add(iItr.next());
    }

    @Override
    public boolean hasNext() {
        if(pQueue.isEmpty())
            addElement();

        return pQueue.size() > 0;
    }

    @Override
    public T peek() {
        if(pQueue.isEmpty())
            addElement();
        if(pQueue.size() == 0)
            throw new NoSuchElementException("No elements availale to be peeked.");
        return pQueue.peek();
    }

    @Override
    public T next() {
        if(pQueue.size() == 0)
            throw new NoSuchElementException("No elements availale to be returned.");
        return pQueue.remove();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("This operation is not supported.");
    }
}
