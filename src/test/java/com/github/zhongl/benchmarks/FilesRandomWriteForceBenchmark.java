/*
 * Copyright 2011 zhongl
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

package com.github.zhongl.benchmarks;

import com.github.zhongl.util.FileBase;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class FilesRandomWriteForceBenchmark extends FileBase {
    public static final int FOUR = 4;
    private final int size = Integer.getInteger("forcing.benchmark.size", 1024);
    private final int averageForceElpasePages = Integer.getInteger("forcing.benchmark.average.force.elpase.pages", 256);
    private long elapse;
    private Random random;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        dir = testDir("benchmark");
        dir.mkdirs();
        random = new Random();
    }

    @Test
    public void benchmark() throws Exception {
        for (int i = 0; i < 65536; i++) {
            byte[] bytes = new byte[4096];
            ByteBuffer.wrap(bytes).putInt(0, random.nextInt());
            int index = random.nextInt(size);
            flushAndIncreaseElpase(index, bytes);
            if ((i + 1) % (FOUR * averageForceElpasePages) == 0) printAverageForceElapse(i + 1);
        }

    }

    private void printAverageForceElapse(int i) {
        System.out.println(String.format("%1$ 5dK: %2$,dns", i, elapse / averageForceElpasePages));
        elapse = 0;
    }

    private void flushAndIncreaseElpase(int index, byte[] bytes) throws IOException {
        long begin = System.nanoTime();
        File to = new File(dir, index + "");
        if (to.exists()) {
//            System.out.println("overwrite " + to);
            Preconditions.checkState(to.delete());
        }
        Files.write(bytes, to);
        elapse += System.nanoTime() - begin;
    }

}
