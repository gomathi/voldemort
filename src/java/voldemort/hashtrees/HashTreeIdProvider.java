package voldemort.hashtrees;

import java.util.List;

import voldemort.utils.ByteArray;

/**
 * Each node maintains primary partition, and set of secondary
 * partitions(primary partitions of other nodes). It is necessary that we
 * maintain separate hash tree for each partition. In HashTree terms, partition
 * id corresponds to a tree id. When a key update comes to the
 * {@link HashTreeImpl}, it needs to know a tree id(partition no) for the key.
 * 
 * This interface defines methods which will be used by {@link HashTreeImpl}
 * class. The implementation has to be thread safe.
 * 
 */
public interface HashTreeIdProvider {

    int getTreeId(ByteArray key);

    List<Integer> getAllTreeIds();

}
