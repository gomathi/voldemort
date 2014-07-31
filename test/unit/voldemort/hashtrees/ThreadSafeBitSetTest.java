package voldemort.hashtrees;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.utils.ThreadSafeBitSet;

public class ThreadSafeBitSetTest {

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testForInvalidArguments() {
        ThreadSafeBitSet obj = new ThreadSafeBitSet(5);
        obj.get(6);
    }

    @Test
    public void testSetBit() {
        ThreadSafeBitSet obj = new ThreadSafeBitSet(5);
        obj.set(0);
        Assert.assertTrue(obj.get(0));
        obj.clear(0);
        Assert.assertFalse(obj.get(0));

        obj = new ThreadSafeBitSet(1024);
        obj.set(1023);
        Assert.assertTrue(obj.get(1023));
    }

    @Test
    public void testClearAndGetSetBits() {
        ThreadSafeBitSet obj = new ThreadSafeBitSet(1000);
        Set<Integer> randomNos = new HashSet<Integer>();
        Random random = new Random();
        for(int i = 0; i < 500; i++)
            randomNos.add(random.nextInt(500));
        for(int value: randomNos)
            obj.set(value);

        List<Integer> sortedRandomNos = new ArrayList<Integer>(randomNos);
        Collections.sort(sortedRandomNos);

        Iterator<Integer> itr = sortedRandomNos.iterator();
        int actualCount = 0;
        for(int i = obj.clearAndGetNextSetBit(0); i >= 0; i = obj.clearAndGetNextSetBit(i + 1)) {
            Assert.assertEquals((int) itr.next(), i);
            actualCount++;
        }
        Assert.assertEquals(randomNos.size(), actualCount);
    }

    @Test
    public void testClearAndGetSetBitsForEmptyArgs() {
        ThreadSafeBitSet obj = new ThreadSafeBitSet(1000);

        int actualCount = 0;
        for(int i = obj.clearAndGetNextSetBit(0); i >= 0; i = obj.clearAndGetNextSetBit(i + 1)) {
            actualCount++;
        }

        Assert.assertEquals(0, actualCount);
    }

    @Test
    public void testCompareAndSet() {
        ThreadSafeBitSet obj = new ThreadSafeBitSet(64);
        obj.set(0);
        Assert.assertTrue(obj.compareAndSet(0, true, false));
        obj.set(0);
        Assert.assertFalse(obj.compareAndSet(0, false, true));
        Assert.assertTrue(obj.compareAndSet(0, true, true));
    }

}
