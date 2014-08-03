package voldemort.hashtrees;

import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.junit.Test;

public class BGStoppableTaskTest {

    private static class ExtendedBGStoppableTask extends BGStoppableTask {

        volatile boolean started = false;
        volatile boolean ran = false;
        volatile boolean finished = false;

        public ExtendedBGStoppableTask(CountDownLatch shutdownLatch) {
            super(shutdownLatch);
        }

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
        ExtendedBGStoppableTask task = new ExtendedBGStoppableTask(latch);
        task.stop();

        new Thread(task).start();
        while(!task.started)
            Thread.sleep(50);
        Assert.assertTrue(task.finished);
        Assert.assertFalse(task.ran);

        latch = new CountDownLatch(1);
        task = new ExtendedBGStoppableTask(latch);

        new Thread(task).start();
        while(!task.started)
            Thread.sleep(50);
        task.stop();
        latch.await();
        Assert.assertTrue(task.finished);
        Assert.assertTrue(task.ran);
    }
}
