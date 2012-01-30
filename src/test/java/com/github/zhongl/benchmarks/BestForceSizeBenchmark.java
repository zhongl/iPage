package com.github.zhongl.benchmarks;

import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static java.text.MessageFormat.format;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BestForceSizeBenchmark extends FileTestContext {

    public static final int BLOCK_SIZE = Integer.getInteger("best.force.size.benchmark.block.size", 4096);
    public static final int SAMPLE_TIMES = Integer.getInteger("best.force.size.benchmark.sample.times", 10);
    public static final int MAX_BLOCKS_SIZE = Integer.getInteger("best.force.size.benchmark.max.blocks.size", 16);

    @Test
    public void heapBufferSizeUseChannelForce() throws Exception {
        file = testFile("heapBufferSizeUseChannelForce");

        final FileChannel channel = new FileOutputStream(file).getChannel();

        new BenchmarkRunner("heapBufferSizeUseChannelForce", new Force() {
            @Override
            public void force(ByteBuffer buffer) throws Exception {
                writeAndForce(channel, buffer);
            }

            @Override
            public ByteBuffer allocate(int size) {
                return ByteBuffer.allocate(size);
            }
        }).run();

        channel.close();
    }

    @Test
    public void directBufferSizeUseChannelForce() throws Exception {
        file = testFile("directBufferSizeUseChannelForce");

        final FileChannel channel = new FileOutputStream(file).getChannel();

        new BenchmarkRunner("directBufferSizeUseChannelForce", new Force() {
            @Override
            public void force(ByteBuffer buffer) throws Exception {
                writeAndForce(channel, buffer);
            }

            @Override
            public ByteBuffer allocate(int size) {
                return ByteBuffer.allocateDirect(size);
            }
        }).run();

        channel.close();
    }

    @Test
    public void bufferSizeUseMappedByteBufferForce() throws Exception {
        file = testFile("bufferSizeUseMappedByteBufferForce");
        final FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

        new BenchmarkRunner("bufferSizeUseMappedByteBufferForce", new Force() {
            @Override
            public void force(ByteBuffer buffer) throws Exception {
                long length = buffer.limit() - buffer.position();
                MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, length);
                mappedByteBuffer.put(buffer);
                mappedByteBuffer.force();
            }

            @Override
            public ByteBuffer allocate(int size) {
                return ByteBuffer.allocate(size);
            }
        }).run();

        channel.close();
    }

    private interface Force {
        void force(ByteBuffer buffer) throws Exception;

        ByteBuffer allocate(int size);
    }

    private static class BenchmarkRunner {

        private final Force delegate;
        private final String name;

        private BenchmarkRunner(String name, Force delegate) {
            this.name = name;
            this.delegate = delegate;
        }

        public void run() throws Exception {
            Collection<ForceSizeAndElapse> forceSizeAndElapses = new ArrayList<ForceSizeAndElapse>(MAX_BLOCKS_SIZE);

            System.out.println("Benchmark: " + name);
            System.out.println();
            for (int size = 1; size <= MAX_BLOCKS_SIZE; size++) {
                int forceSize = BLOCK_SIZE * size;
                ByteBuffer buffer = delegate.allocate(forceSize);
                long begin = System.nanoTime();
                for (int i = 0; i < SAMPLE_TIMES; i++) {
                    buffer.rewind();
                    delegate.force(buffer);
                }
                long end = System.nanoTime();
                ForceSizeAndElapse forceSizeAndElapse = new ForceSizeAndElapse(end - begin, forceSize);
                System.out.println(forceSizeAndElapse);
                forceSizeAndElapses.add(forceSizeAndElapse);
            }

            ForceSizeAndElapse best = Collections.max(forceSizeAndElapses);
            System.out.println("=======================================");
            System.out.println("The winner is : " + best);

        }
    }

    private static class ForceSizeAndElapse implements Comparable<ForceSizeAndElapse> {
        final int forceSize;
        final double elpase;

        private ForceSizeAndElapse(long elpase, int forceSize) {
            this.elpase = elpase;
            this.forceSize = forceSize;
        }

        public double ratio() {
            return forceSize / elpase / SAMPLE_TIMES;
        }

        @Override
        public int compareTo(ForceSizeAndElapse o) {
            if (ratio() > o.ratio()) return 1;
            if (ratio() < o.ratio()) return -1;
            return 0;
        }

        @Override
        public String toString() {
            return format(
                    "force size={0, number}({1, number} KB)," +
                            " avg elapse={2, number}ns," +
                            " speed={3, number, #.##}MB/S",
                    forceSize,
                    forceSize / 1024,
                    elpase / SAMPLE_TIMES,
                    (forceSize / 1024 * 1.0 /1024) / (elpase / SAMPLE_TIMES) * TimeUnit.SECONDS.toNanos(1));
        }
    }

    private void writeAndForce(FileChannel channel, ByteBuffer buffer) throws IOException {
        channel.write(buffer);
        channel.force(false);
    }
}
