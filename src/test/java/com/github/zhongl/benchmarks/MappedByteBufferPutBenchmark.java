package com.github.zhongl.benchmarks;

import com.github.zhongl.ex.nio.DirectByteBufferCleaner;
import com.github.zhongl.util.FileTestContext;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedByteBufferPutBenchmark extends FileTestContext {

    public static final int UNIT = 4096;
    public static final int QUANTITY = 100;

    @Test
    public void putOne() throws Exception {
        file = testFile("putOne");
        final MappedByteBuffer mappedByteBuffer = Files.map(file, FileChannel.MapMode.READ_WRITE, UNIT * QUANTITY);
        final ByteBuffer buffer = ByteBuffer.allocate(UNIT * QUANTITY);

        new Benchmark(new Runnable() {
            @Override
            public void run() {
                mappedByteBuffer.put(buffer);
            }
        }).run();

        DirectByteBufferCleaner.clean(mappedByteBuffer);
    }

    @Test
    public void putArray() throws Exception {
        file = testFile("putArray");
        final MappedByteBuffer mappedByteBuffer = Files.map(file, FileChannel.MapMode.READ_WRITE, UNIT * QUANTITY);
        final ByteBuffer[] buffers = new ByteBuffer[QUANTITY];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocate(UNIT);
        }

        new Benchmark(new Runnable() {
            @Override
            public void run() {
                for (ByteBuffer buffer : buffers) {
                    mappedByteBuffer.put(buffer);
                }
            }
        }).run();

        DirectByteBufferCleaner.clean(mappedByteBuffer);
    }

    private class Benchmark implements Runnable {
        private final Runnable runnable;

        public Benchmark(Runnable runnable) {this.runnable = runnable;}

        @Override
        public void run() {
            Stopwatch stopwatch = new Stopwatch().start();
            runnable.run();
            stopwatch.stop();
            System.out.println(stopwatch);
        }
    }
}
