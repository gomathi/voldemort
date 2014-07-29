package voldemort.hashtrees;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicIntegerArray;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * Default {@link BitSet} provided in java is not thread safe. This class
 * provides a minimalistic thread safe version of BitSet.
 * 
 * Unlike BitSet, this class does not grow automatically, the BitSet size is
 * fixed.
 * 
 */
@Threadsafe
public class ThreadSafeBitSet {

    private final static int NO_OF_BITS = 32;

    private final int length;
    private final int totBits;
    private final AtomicIntegerArray bitsHolder;

    public ThreadSafeBitSet(int totBits) {
        this.totBits = totBits;
        this.length = calcLength(totBits, NO_OF_BITS);
        this.bitsHolder = new AtomicIntegerArray(length);
    }

    private static int calcLength(int totBits, int bitsPerBlock) {
        return totBits / bitsPerBlock + (((totBits % bitsPerBlock) == 0) ? 0 : 1);
    }

    private static int getNthBit(int value, int pos) {
        value = value >> pos;
        return value & 1;
    }

    /**
     * Sets given bitIndex.
     * 
     * @param bitIndex, can not be negative or larger than the size of length of
     *        the bitset.
     */
    public void set(int bitIndex) {
        int arrIndex = calcLength(bitIndex + 1, NO_OF_BITS) - 1;
        if(arrIndex >= totBits || arrIndex < 0)
            throw new ArrayIndexOutOfBoundsException("Given index is invalid : " + bitIndex);
        int noOfShifts = bitIndex % NO_OF_BITS;
        int value = 1 << noOfShifts;
        while(true) {
            int oldValue = bitsHolder.get(arrIndex);
            int newValue = oldValue | value;
            if(bitsHolder.compareAndSet(arrIndex, oldValue, newValue))
                return;
        }
    }

    /**
     * Clears the first setBit from the startIndex, and returns the index. If
     * there is no such value, returns -1.
     * 
     * @param startIndex, can not be negative or larger than the size of length
     *        of the bitset.
     * @return
     */
    public int clearAndGetNextSetBit(int startIndex) {
        if(startIndex < 0 || startIndex >= totBits)
            throw new ArrayIndexOutOfBoundsException("Given index is invalid : " + startIndex);

        int localStartInd = startIndex % NO_OF_BITS;
        int localEndInd = NO_OF_BITS - 1;
        int endIndex = (totBits - 1) % NO_OF_BITS;
        int arrStartPos = calcLength(startIndex + 1, NO_OF_BITS) - 1;
        int arrEndPos = bitsHolder.length() - 1;

        for(int i = arrStartPos; i <= arrEndPos; i++) {
            if(i == arrEndPos)
                localEndInd = endIndex;
            for(int j = localStartInd; j <= localEndInd; j++) {
                int value = ~(1 << j);
                int oldValue = bitsHolder.get(i);
                while(getNthBit(oldValue, j) == 1) {
                    int newValue = oldValue & value;
                    if(bitsHolder.compareAndSet(i, oldValue, newValue))
                        return i;
                    oldValue = bitsHolder.get(i);
                }
            }
            localStartInd = 0;
            localEndInd = NO_OF_BITS - 1;
        }
        return -1;
    }

    /**
     * Clears the given bitIndex.
     * 
     * @param bitIndex, can not be negative or greater than length of the
     *        bitSet.
     */
    public void clear(int bitIndex) {
        if(bitIndex >= totBits || bitIndex < 0)
            throw new ArrayIndexOutOfBoundsException("Given index is invalid : " + bitIndex);

        int arrIndex = calcLength(bitIndex + 1, NO_OF_BITS) - 1;
        int noOfShifts = bitIndex % NO_OF_BITS;
        int value = ~(1 << noOfShifts);
        while(true) {
            int oldValue = bitsHolder.get(arrIndex);
            int newValue = oldValue & value;
            if(bitsHolder.compareAndSet(arrIndex, oldValue, newValue))
                return;
        }
    }
}
