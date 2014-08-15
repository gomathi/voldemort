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
