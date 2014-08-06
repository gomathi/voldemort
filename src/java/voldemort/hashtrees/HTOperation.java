package voldemort.hashtrees;

/**
 * Used to tag type of operation when the input is fed into the non blocking
 * version of {@link HashTreeImpl} hPut and hRemove methods.
 * 
 */
enum HTOperation {
    PUT,
    REMOVE,
    STOP
}
