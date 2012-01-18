package com.github.zhongl.kvengine;

import com.github.zhongl.nio.CommonAccessors;
import com.github.zhongl.util.FileBase;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DetectMemoryLeak extends FileBase {


    @Test
    public void putAndGetAndRemove() throws Exception {
        Runtime runtime = Runtime.getRuntime();
        System.out.println(runtime.maxMemory() / 1024 / 1024);

        dir = testDir("putAndGetAndRemove");
        final KVEngine engine = KVEngine.baseOn(dir)
                .groupCommit(false)
                .valueAccessor(CommonAccessors.BYTES)
                .flushCount(10000)
                .flushElapseMilliseconds(1000L)
                .build();


        int size = 1000000;
        final CountDownLatch latch = new CountDownLatch(size);

        ExecutorService service = Executors.newFixedThreadPool(runtime.availableProcessors() * 2);


        for (int i = 0; i < size; i++) {
            final byte[] value = new byte[4096];
            ByteBuffer.wrap(value).putInt(i);
            service.submit(new Runnable() {
                @Override
                public void run() {
                    act(engine, value, latch);
                }
            });
            if (i % 10 == 0) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }
        }

        long count = 0;
        while (count < size) {
            latch.await(3L, TimeUnit.SECONDS);
            count = latch.getCount();
            System.out.println(count);
        }
        service.shutdownNow();
    }

    private void act(final KVEngine engine, final byte[] value, final CountDownLatch latch) {
        final Md5Key key = Md5Key.generate(value);
        engine.put(key, value, new FutureCallback<byte[]>() {
            @Override
            public void onSuccess(byte[] result) {
                engine.get(key, new FutureCallback<byte[]>() {
                    @Override
                    public void onSuccess(byte[] result) {
                        if (Arrays.equals(result, value)) engine.remove(key, new FutureCallback<byte[]>() {
                            @Override
                            public void onSuccess(byte[] result) {
                                latch.countDown();
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                t.printStackTrace();
                            }
                        });
                        else System.err.println("get invalid value.");
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        t.printStackTrace();
                    }
                });
            }

            @Override
            public void onFailure(Throwable t) {
                t.printStackTrace();
            }
        });
    }
}
