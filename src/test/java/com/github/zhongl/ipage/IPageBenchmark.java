package com.github.zhongl.ipage;

import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.Md5;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageBenchmark extends FileTestContext {

    private IPage<Integer, byte[]> iPage;
    private ExecutorService service;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        int threads = Runtime.getRuntime().availableProcessors() * 2;
        service = new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(100),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Test
    public void benchmark() throws Exception {
        dir = testDir("benchmark");

        int ephemeronThroughout = 100000;
        long flushMillis = 5000L;
        int flushCount = 10000;

        iPage = new IPage<Integer, byte[]>(dir, new BytesCodec(), ephemeronThroughout, flushMillis, flushCount) {
            @Override
            protected Key transform(Integer key) {
                return new Key(Md5.md5(key.toString().getBytes()));
            }
        };

        final int times = 1000000;
        final CountDownLatch latch = new CountDownLatch(times);

        Benchmarks.benchmark("add", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < times; i++) {
                    final int num = i;
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            iPage.add(num, new byte[1024], new FutureCallback<Void>() {
                                @Override
                                public void onSuccess(Void result) {
                                    latch.countDown();
                                }

                                @Override
                                public void onFailure(Throwable t) {
                                    t.printStackTrace();
                                }
                            });
                        }
                    });
                }

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, times);
    }


    @Override
    @After
    public void tearDown() throws Exception {
        iPage.stop();
        service.shutdownNow();
        super.tearDown();
    }
}
