package voldemort.hashtrees;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.utils.AtomicBitSet;

public class AtomicBitSetTest {

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testForInvalidArguments() {
        AtomicBitSet obj = new AtomicBitSet(5);
        obj.get(6);
    }

    @Test
    public void testSetBit() {
        AtomicBitSet obj = new AtomicBitSet(5);
        obj.set(0);
        Assert.assertTrue(obj.get(0));
        obj.clear(0);
        Assert.assertFalse(obj.get(0));

        obj = new AtomicBitSet(1024);
        obj.set(1023);
        Assert.assertTrue(obj.get(1023));
    }

}
