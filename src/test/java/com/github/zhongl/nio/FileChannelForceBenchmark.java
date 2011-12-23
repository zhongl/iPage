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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class FileChannelForceBenchmark extends FileBase {
    public static final int ONE_KILO_BYTES = 1024;
    public static final int FOUR = 4;
    private final int size = Integer.getInteger("forcing.benchmark.size", 1024);
    private final int pages = Integer.getInteger("forcing.benchmark.pages", 1);
    private final int averageForceElpasePages = Integer.getInteger("forcing.benchmark.average.force.elpase.pages", 256);
    private FileChannel channel;
    private long elapse;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        file = testFile("benchmark");
        channel = (FileChannel) Channels.newChannel(new FileOutputStream(file));
    }

    @Test
    public void benchmark() throws Exception {
        for (int i = 0; i < size; i++) {
            byte[] bytes = new byte[1024];
            channel.position(i * ONE_KILO_BYTES);
            channel.write(ByteBuffer.wrap(bytes).putInt(0, i));
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
        channel.force(true);
        elapse += System.nanoTime() - begin;
    }

    @Override
    @After
    public void tearDown() throws Exception {
        channel.close();
        super.tearDown();
    }
}
