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

import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.junit.Test;

import voldemort.hashtrees.tasks.BGStoppableTask;

public class BGStoppableTaskTest {

    private static class ExtendedBGStoppableTask extends BGStoppableTask {

        volatile boolean started = false;
        volatile boolean ran = false;
        volatile boolean finished = false;

        @Override
        public void run() {
            started = true;
            if(enableRunningStatus()) {
                try {} finally {
                    disableRunningStatus();
                }
                ran = true;
            }
            finished = true;
        }

    }

    @Test
    public void testStoppableTask() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        ExtendedBGStoppableTask task = new ExtendedBGStoppableTask();
        task.stop(latch);

        new Thread(task).start();
        while(!task.started)
            Thread.sleep(50);
        Assert.assertTrue(task.finished);
        Assert.assertFalse(task.ran);

        latch = new CountDownLatch(1);
        task = new ExtendedBGStoppableTask();

        new Thread(task).start();
        while(!task.started)
            Thread.sleep(50);
        task.stop(latch);
        latch.await();
        Assert.assertTrue(task.finished);
        Assert.assertTrue(task.ran);
    }
}
