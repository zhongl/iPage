package com.github.zhongl.ex.nio;

import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Closeables;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ThreadAndChannelWritingBenchmark extends FileTestContext {

    public static final int SIZE = 64;
    private Runnable writing;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = testDir("0");
        writing = new Runnable() {
            @Override
            public void run() {
                long l = System.nanoTime();
                FileChannel channel = FileChannels.getOrOpen(new File(dir, l + ""));
                try {
                    for (int i = 0; i < SIZE; i++) {
                        channel.write(ByteBuffer.allocate(1024 * 1024));
                    }
                    channel.force(false);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Closeables.closeQuietly(channel);
            }
        };
    }

    @Test
    public void singleThreadWrite() throws Exception {
        Benchmarks.benchmark("single thread: ", writing, 1);
    }

    @Test
    public void multipThreadsWrite() throws Exception {
        final int num = Runtime.getRuntime().availableProcessors() * 2;
        final CountDownLatch latch = new CountDownLatch(num);

        final Runnable wrap = new Runnable() {

            @Override
            public void run() {
                writing.run();
                latch.countDown();
            }
        };

        Benchmarks.benchmark("multip threads: ", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < num; i++) {
                    new Thread(wrap).start();
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();  // TODO right
                }
            }
        }, num);
    }
}
