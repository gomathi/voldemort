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

import voldemort.utils.Pair;

public interface Storage {

    ByteBuffer get(ByteBuffer key);

    void put(ByteBuffer key, ByteBuffer value) throws Exception;

    ByteBuffer remove(ByteBuffer key) throws Exception;

    Iterator<Pair<ByteBuffer, ByteBuffer>> iterator();
}
