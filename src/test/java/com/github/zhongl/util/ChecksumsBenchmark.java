package com.github.zhongl.util;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChecksumsBenchmark {

    private abstract static class DoChecksum {
        final String name;

        protected DoChecksum(String name) {this.name = name;}

        public abstract void checksum(ByteBuffer buffer);
    }

    private static class Runner {

        private final ByteBuffer buffer;
        private final DoChecksum doChecksum;

        public Runner(ByteBuffer buffer, DoChecksum doChecksum) {
            this.buffer = buffer;
            this.doChecksum = doChecksum;
        }

        public void run() {
            Stopwatch stopwatch = new Stopwatch().start();
            doChecksum.checksum(buffer);
            stopwatch.stop();

            long elapse = stopwatch.elapsedTime(TimeUnit.NANOSECONDS);
            println(doChecksum.name, elapse);
        }

        private void println(String name, long elapse) {
            System.out.println(MessageFormat.format("{0} : {1, number}ns", name, elapse));
        }
    }

    @Test
    public void oneByOneHeap() throws Exception {
        new Runner(ByteBuffer.allocate(4096), new DoChecksum("oneByOneHeap") {

            @Override
            public void checksum(ByteBuffer buffer) {
                Checksums.checksum_1b1(buffer);
            }
        }).run();
    }

    @Test
    public void oneByOneDirect() throws Exception {
        new Runner(ByteBuffer.allocateDirect(4096), new DoChecksum("oneByOneDirect") {

            @Override
            public void checksum(ByteBuffer buffer) {
                Checksums.checksum_1b1(buffer);
            }
        }).run();
    }

    @Test
    public void toBytesDirect() throws Exception {
        new Runner(ByteBuffer.allocateDirect(4096), new DoChecksum("toBytesDirect") {

            @Override
            public void checksum(ByteBuffer buffer) {
                Checksums.checksum_bytes(buffer);
            }
        }).run();
    }

    @Test
    public void toBytesHeap() throws Exception {
        new Runner(ByteBuffer.allocate(4096), new DoChecksum("toBytesHeap") {

            @Override
            public void checksum(ByteBuffer buffer) {
                Checksums.checksum_bytes(buffer);
            }
        }).run();
    }

    @Test
    public void mapBytesHeap() throws Exception {
        new Runner(ByteBuffer.allocate(4096), new DoChecksum("mapBytesHeap") {

            @Override
            public void checksum(ByteBuffer buffer) {
                Checksums.checksum_direct_or_bytes(buffer);
            }
        }).run();
    }

}
