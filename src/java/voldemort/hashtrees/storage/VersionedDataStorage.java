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
package voldemort.hashtrees.storage;

import java.nio.ByteBuffer;
import java.util.Iterator;

import voldemort.hashtrees.thrift.generated.VersionedData;

/**
 * Stores (key,value) with a monotonically increasing number. Using this number
 * clients can request for all the changes happened since this number.
 * 
 */
public interface VersionedDataStorage {

    VersionedData versionedPut(int treeId, ByteBuffer key, ByteBuffer value);

    VersionedData versionedRemove(int treeId, ByteBuffer key);

    Iterator<VersionedData> getVersionedData(int treeId);

    Iterator<VersionedData> getVersionedData(int treeId, long fromVersionNo);
}
