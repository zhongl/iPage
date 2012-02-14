/*
 * Copyright 2012 zhongl
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.ipage;

import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.Md5;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageBenchmark extends FileTestContext {

    public static final int EPHEMERON_THROUGHOUT = Integer.getInteger("ipage.benchmark.throughout", 100000);
    public static final long FLUSH_MILLIS = Long.getLong("ipage.benchmark.flush.millis", 5000L);
    public static final int FLUSH_COUNT = Integer.getInteger("ipage.benchmark.flush.count", 10000);
    private IPage<Integer, byte[]> iPage;
    private ExecutorService service;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        int threads = Runtime.getRuntime().availableProcessors();
        service = new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Test
    public void addThenRemove() throws Exception {
        dir = testDir("addThenRemove");

        initIPage(100000, 5000L, 10000);

        final int times = 1000000;
        final CountDownLatch aLatch = new CountDownLatch(times);

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
                                    aLatch.countDown();
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
                    aLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, times);

        final CountDownLatch rLatch = new CountDownLatch(times);
        Benchmarks.benchmark("remove", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < times; i++) {
                    final int num = i;
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            iPage.remove(num, new FutureCallback<Void>() {
                                @Override
                                public void onSuccess(Void result) {
                                    rLatch.countDown();
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
                    rLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, times);


    }

    private void initIPage(final int ephemeronThroughout, final long flushMillis, final int flushCount) throws Exception {
        iPage = new IPage<Integer, byte[]>(dir, new BytesCodec(), ephemeronThroughout, flushMillis, flushCount) {
            @Override
            protected Key transform(Integer key) {
                return new Key(Md5.md5(key.toString().getBytes()));
            }
        };
    }

    @Test
    public void get() throws Exception {
        dir = testDir("get");

        initIPage(EPHEMERON_THROUGHOUT, FLUSH_MILLIS, FLUSH_COUNT);

        final int times = 100000;
        final CountDownLatch aLatch = new CountDownLatch(times);

        for (int i = 0; i < times; i++) {
            final int num = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    iPage.add(num, new byte[1024], new FutureCallback<Void>() {
                        @Override
                        public void onSuccess(Void result) {
                            aLatch.countDown();
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
            aLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final Random random = new Random();

        final CountDownLatch gLatch = new CountDownLatch(times * 10);
        Benchmarks.benchmark("get", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < times * 10; i++) {
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            iPage.get(random.nextInt(times));
                            gLatch.countDown();
                        }
                    });
                }

                try {
                    gLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();  // TODO right
                }
            }
        }, times * 10);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        iPage.stop();
        service.shutdownNow();
        super.tearDown();
    }
}
