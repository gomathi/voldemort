package voldemort.utils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;

import voldemort.annotations.concurrency.Threadsafe;

/**
 * Default {@link BitSet} provided in java is not thread safe. This class
 * provides a minimalistic thread safe version of BitSet.
 * 
 * This class does not grow automatically.
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
        return (totBits / bitsPerBlock) + (((totBits % bitsPerBlock) == 0) ? 0 : 1);
    }

    /**
     * 
     * @param value
     * @param pos
     * @return
     */
    private static int getNthBit(int value, int pos) {
        value = value >> pos;
        return value & 1;
    }

    private void validateArgument(int bitIndex) {
        if(bitIndex >= totBits || bitIndex < 0)
            throw new ArrayIndexOutOfBoundsException("Given index is invalid : " + bitIndex);
    }

    /**
     * Gets the index of the element in {@link #bitsHolder} for the given
     * bitIndex.
     * 
     * @param bitIndex, values can be between 0 and {@link #totBits} - 1
     * @return
     */
    private int getArrayIndex(int bitIndex) {
        return calcLength(bitIndex + 1, NO_OF_BITS) - 1;
    }

    /**
     * Sets given bitIndex.
     * 
     * @param bitIndex, can not be negative or larger than or equal to the
     *        length of the bitSet.
     */
    public void set(int bitIndex) {
        validateArgument(bitIndex);

        int arrIndex = getArrayIndex(bitIndex);
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
     * Gets the value of bitIndex.
     * 
     * @param bitIndex, can not be negative or larger than or equal to the
     *        length of the bitSet.
     * @return, true corresponds to setBit and false corresponds to clearBit
     */
    public boolean get(int bitIndex) {
        validateArgument(bitIndex);

        int pos = bitIndex % NO_OF_BITS;
        int arrIndex = getArrayIndex(bitIndex);
        return getNthBit(bitsHolder.get(arrIndex), pos) == 1;
    }

    /**
     * Clears the first setBit from the startIndex, and returns the index. If
     * there is no such value, returns -1.
     * 
     * @param bitIndex
     * @return, cleared setBit index.
     */
    public int clearAndGetNextSetBit(int startIndex) {
        int localStartInd = startIndex % NO_OF_BITS;
        int localEndInd = NO_OF_BITS - 1;
        int endIndex = (totBits - 1) % NO_OF_BITS;
        int arrStartPos = getArrayIndex(startIndex);
        int arrEndPos = bitsHolder.length() - 1;
        int currPos = startIndex;

        for(int i = arrStartPos; i <= arrEndPos; i++) {
            if(i == arrEndPos)
                localEndInd = endIndex;
            for(int j = localStartInd; j <= localEndInd; j++) {
                if(compareAndSet(i, j, true, false))
                    return currPos;
                currPos++;
            }
            localStartInd = 0;
            localEndInd = NO_OF_BITS - 1;
        }
        return -1;
    }

    public List<Integer> clearAndGetAllSetBits() {
        List<Integer> result = new ArrayList<Integer>();
        for(int i = 0; i < bitsHolder.length(); i++) {
            int oldValue = bitsHolder.get(i);
            while(!bitsHolder.compareAndSet(i, oldValue, 0))
                oldValue = bitsHolder.get(i);
            for(int j = i * 32, max = ((i * 32) + 32); j < max && j < totBits && oldValue != 0; j++) {
                if((oldValue & 1) == 1)
                    result.add(j);
                oldValue = oldValue >> 1;
            }
        }
        return result;
    }

    public boolean compareAndSet(int bitIndex, boolean expectedVal, boolean value) {
        validateArgument(bitIndex);

        int arrIndex = getArrayIndex(bitIndex);
        int noOfShifts = bitIndex % NO_OF_BITS;
        return compareAndSet(arrIndex, noOfShifts, expectedVal, value);
    }

    private boolean compareAndSet(int arrIndex, int bitIndex, boolean expectedVal, boolean value) {
        int concValue = (value) ? (1 << bitIndex) : (~(1 << bitIndex));
        while(true) {
            int oldValue = bitsHolder.get(arrIndex);
            boolean currValue = getNthBit(oldValue, bitIndex) == 1;
            if(currValue == expectedVal) {
                int newValue = (value) ? (oldValue | concValue) : (oldValue & concValue);
                if(bitsHolder.compareAndSet(arrIndex, oldValue, newValue))
                    return true;
            } else
                return false;
        }
    }

    /**
     * Clears the given bitIndex.
     * 
     * @param bitIndex, can not be negative or larger than or equal to the
     *        length of the bitSet.
     */
    public void clear(int bitIndex) {
        validateArgument(bitIndex);

        int arrIndex = getArrayIndex(bitIndex);
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
