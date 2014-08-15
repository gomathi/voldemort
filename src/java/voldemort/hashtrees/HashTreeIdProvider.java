/*
 * Copyright 2008-2014 LinkedIn, Inc
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
package voldemort.hashtrees;

import java.nio.ByteBuffer;
import java.util.List;

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

    /**
     * Returned treeId should be a positive value.
     * 
     * @param key
     * @return
     */
    int getTreeId(ByteBuffer key);

    List<Integer> getAllTreeIds();

}
