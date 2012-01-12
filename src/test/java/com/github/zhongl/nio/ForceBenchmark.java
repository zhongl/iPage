/*
 * Copyright 2012 zhongl
 *
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

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.MessageFormat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ForceBenchmark extends FileBase {

    private static final int PAGE_SIZE = Integer.getInteger("force.benchmark.page.size", 8192);
    private static final int LENGTH = Integer.getInteger("force.benchmark.MB", 64) * 1024 * 1024;
    private static final byte[] BYTES = new byte[PAGE_SIZE];

    @Test
    public void showPageSize() throws Exception {
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                try {
                    Class<?> aClass = Class.forName("sun.misc.Unsafe");
                    Constructor<?> constructor = aClass.getDeclaredConstructor();
                    constructor.setAccessible(true);
                    Object unsafe = constructor.newInstance();
                    Object pageSize = aClass.getMethod("pageSize").invoke(unsafe);
                    System.out.println(pageSize);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    @Test
    public void forceMappedOnce() throws Exception {
        file = testFile("forceMappedOnce");
        MappedByteBuffer buffer = Files.map(file, FileChannel.MapMode.READ_WRITE, LENGTH);

        long begin = System.nanoTime();
        for (int i = 0; i < LENGTH / PAGE_SIZE; i++) {
            buffer.put(BYTES);
        }
        buffer.force();
        System.out.println(MessageFormat.format("{0, number}ns", System.nanoTime() - begin));

        FileChannel source = new FileInputStream(file).getChannel();
        File t = testFile("t");
        FileChannel target = new FileOutputStream(t).getChannel();

        begin = System.nanoTime();
        source.transferTo(0, LENGTH, target);
        System.out.println(MessageFormat.format("{0, number}ns", System.nanoTime() - begin));

        source.close();
        target.close();

        t.delete();
        DirectByteBufferCleaner.clean(buffer);
    }

    @Test
    public void forceMappedEveryTime() throws Exception {
        file = testFile("forceMappedEveryTime");
        MappedByteBuffer buffer = Files.map(file, FileChannel.MapMode.READ_WRITE, LENGTH);
        for (int i = 0; i < LENGTH / PAGE_SIZE; i++) {
            buffer.put(new byte[PAGE_SIZE]);
            buffer.force();
        }
        DirectByteBufferCleaner.clean(buffer);
    }

    @Test
    public void forceChannelOnce() throws Exception {
        file = testFile("forceChannelOnce");
        FileChannel channel = new FileOutputStream(file).getChannel();

        long begin = System.nanoTime();
        for (int i = 0; i < LENGTH / PAGE_SIZE; i++) {
            channel.write(ByteBuffer.wrap(new byte[PAGE_SIZE]));
        }
        channel.force(false);
        System.out.println(MessageFormat.format("{0, number}ns", System.nanoTime() - begin));

        channel.close();
    }
}
