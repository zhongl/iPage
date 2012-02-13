/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.util;

import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileAsserter {
    private final File file;

    private FileAsserter(File file) {
        this.file = file;
    }

    public static FileAsserter assertExist(File file) {
        return new FileAsserter(file);
    }

    public static byte[] string(String value) {
        return value.getBytes();
    }

    public static byte[] length(int value) {
        return Ints.toByteArray(value);
    }

    public void contentIs(byte[]... bytesArray) throws IOException {
        byte[] expect = Bytes.concat(bytesArray);
        byte[] actual = Arrays.copyOf(Files.toByteArray(file), expect.length);
        assertThat(actual, is(expect));
    }
}
