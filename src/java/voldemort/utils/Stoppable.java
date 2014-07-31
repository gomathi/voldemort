package voldemort.utils;

/**
 * Indicates a class that accepts a stop signal, and does a clean shut down
 * operation after getting a stop signal.
 * 
 */

public interface Stoppable {

    void stop();
}
