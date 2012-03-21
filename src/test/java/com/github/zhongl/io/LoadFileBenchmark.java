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

package com.github.zhongl.io;

import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class LoadFileBenchmark {
    private static final int SIZE = 1024 * 1024 * 64;
    private static final int TIMES = 10;

    private static File file;
    private static File dir;

    @BeforeClass
    public static void setUpClass() throws Exception {
        dir = new File("target/tmp-test-io/" + LoadFileBenchmark.class.getSimpleName());
        dir.mkdirs();
        file = new File(dir, "load");
        Files.write(new byte[SIZE], file);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        FileTestContext.delete(dir);
    }

    @Test
    public void map() throws Exception {
        Benchmarks.benchmark("map", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < TIMES; i++) {
                    try {
                        Files.map(file, FileChannel.MapMode.PRIVATE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, TIMES);
    }

    @Test
    public void read() throws Exception {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);
        Benchmarks.benchmark("read", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < TIMES; i++) {
                    FileInputStream inputStream = null;
                    try {
                        inputStream = new FileInputStream(file);
                        inputStream.getChannel().read(buffer);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        Closeables.closeQuietly(inputStream);
                    }
                    buffer.clear();
                }
            }
        }, TIMES);
    }
}
