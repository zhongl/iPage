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

import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.text.MessageFormat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ScanFileBenchmark extends FileTestContext {

    @Test
    public void benchmark() throws Exception {
        file = testFile("benchmark");

        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));

        byte[] bytes = new byte[1024];
        ByteBuffer.wrap(bytes).putInt(1020);
        for (int i = 0; i < 1024 * 64; i++) {
            outputStream.write(bytes);
        }

        outputStream.close();

        long begin = System.nanoTime();
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));
        byte[] lengthBytes = new byte[4];
        int read;
        for (; ; ) {
            read = inputStream.read(lengthBytes);
            if (read < 0) break;
            int length = ByteBuffer.wrap(lengthBytes).getInt();
            read = inputStream.read(new byte[length]);
            if (read < 0) break;
        }
        inputStream.close();
        long end = System.nanoTime();
        System.out.println(MessageFormat.format("elpase: {0,Number}ns", (end - begin)));
    }
}
