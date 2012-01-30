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

package com.github.zhongl.benchmarks;

import com.github.zhongl.util.FileTestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class MappedOnForceBenchmark extends FileTestContext {
    public static final int ONE_KILO_BYTES = 1024;
    public static final int FOUR = 4;
    private final int size = Integer.getInteger("forcing.benchmark.size", 1024);
    private final int pages = Integer.getInteger("forcing.benchmark.pages", 1);
    private final int averageForceElpasePages = Integer.getInteger("forcing.benchmark.average.force.elpase.pages", 256);
    private FileChannel channel;
    private long elapse;
    private MappedByteBuffer buffer;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        file = testFile("benchmark");
        channel = new RandomAccessFile(file, "rw").getChannel();
    }

    @Test
    public void benchmark() throws Exception {
        for (int i = 0; i < size; i++) {
            byte[] bytes = new byte[ONE_KILO_BYTES];
            if (i % (FOUR * pages) == 0)
                buffer = channel.map(READ_WRITE, i * ONE_KILO_BYTES * FOUR * pages, ONE_KILO_BYTES * FOUR * pages);

            ByteBuffer.wrap(bytes).putInt(0, i);
            buffer.put(bytes);
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
        buffer.force();
        channel.close();
        super.tearDown();
    }
}
