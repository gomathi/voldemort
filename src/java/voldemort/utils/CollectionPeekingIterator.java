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
