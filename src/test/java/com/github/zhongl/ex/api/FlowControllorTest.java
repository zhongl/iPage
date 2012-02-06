package com.github.zhongl.ex.api;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FlowControllorTest {
    @Test
    public void usage() throws Exception {
        final FlowControllor controllor = new FlowControllor();

        assertThat(controllor.throughout(0), is(10000));
        assertThat(controllor.throughout(-9999), is(1));

        assertThat(controllor.timeout(5), is(100));

        final CyclicBarrier barrier = new CyclicBarrier(2);

        final TimeoutCapture capture1 = new TimeoutCapture(controllor);
        final TimeoutCapture capture2 = new TimeoutCapture(controllor);

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                capture1.run();
            }
        });
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                capture2.run();
            }
        });
        t2.start();

        t1.join();
        t2.join();

        // assert only one of timeout is greater than or equals 5 ms
        assertThat(capture1.timeout() == 0L ?
                capture2.timeout() >= 5L :
                capture2.timeout() == 0L ? capture1.timeout() >= 5L : false, is(true));
    }

    private static class TimeoutCapture implements Runnable {
        private final FlowControllor controllor;
        private volatile long timeout;


        public TimeoutCapture(FlowControllor controllor) {
            this.controllor = controllor;
        }

        @Override
        public void run() {
            Stopwatch stopwatch = new Stopwatch().start();
            try {
                controllor.call(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        Thread.sleep(10L);
                        return null;  // TODO call
                    }
                });
            } catch (InterruptedException e) {
                stopwatch.stop();
                timeout = stopwatch.elapsedMillis();
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
            stopwatch.stop();
        }

        public long timeout() {return timeout;}
    }
}
