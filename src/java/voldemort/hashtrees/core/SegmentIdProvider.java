package voldemort.hashtrees.core;

import java.nio.ByteBuffer;

/**
 * Defines the function to return the segId given the key.
 * 
 */
public interface SegmentIdProvider {

    int getSegmentId(ByteBuffer key);
}