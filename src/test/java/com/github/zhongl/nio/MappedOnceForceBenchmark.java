/*
 * Copyright 2011 zhongl
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

package com.github.zhongl.nio;

import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static com.github.zhongl.nio.DirectByteBufferCleaner.clean;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class MappedOnceForceBenchmark extends FileBase {
    public static final int ONE_KILO_BYTES = 1024;
    public static final int FOUR = 4;
    private final int size = Integer.getInteger("forcing.benchmark.size", 1024);
    private final int pages = Integer.getInteger("forcing.benchmark.pages", 1);
    private final int averageForceElpasePages = Integer.getInteger("forcing.benchmark.average.force.elpase.pages", 256);
    private MappedByteBuffer buffer;
    private long elapse;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        file = testFile("benchmark");
        buffer = Files.map(file, FileChannel.MapMode.READ_WRITE, size * ONE_KILO_BYTES);
    }

    @Test
    public void benchmark() throws Exception {
        for (int i = 0; i < size; i++) {
            byte[] bytes = new byte[1020];
            ByteBuffer.wrap(bytes).putInt(i);
            buffer.position(i * ONE_KILO_BYTES);
            CommonAccessors.BYTES.write(bytes, buffer);
            if ((i + 1) % (FOUR * pages) == 0) flushAndIncreaseElpase();
            if ((i + 1) % (FOUR * averageForceElpasePages) == 0) printAverageForceElapse(i + 1);
        }

    }

    private void printAverageForceElapse(int i) {
        System.out.println(String.format("%1$ 5dK: %2$,dns", i, elapse / averageForceElpasePages));
        elapse = 0;
    }

    private void flushAndIncreaseElpase() throws IOException {
        long begin = System.nanoTime();
        buffer.force();
        elapse += System.nanoTime() - begin;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        clean(buffer);
        super.tearDown();
    }
}
