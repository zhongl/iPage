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

package com.github.zhongl.ipage;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class FileContentAsserter {

    private final byte[] content;

    public static FileContentAsserter of(File file) throws IOException {
        return new FileContentAsserter(file);
    }

    public FileContentAsserter(File file) throws IOException {
        content = Files.toByteArray(file);
    }

    public void assertIs(byte[] expect) {
        byte[] actual = new byte[expect.length];
        System.arraycopy(content, 0, actual, 0, actual.length);
        assertThat(actual, is(expect));
    }

    public void assertIsNot(byte[] expect) {
        byte[] actual = new byte[expect.length];
        System.arraycopy(content, 0, actual, 0, actual.length);
        assertThat(actual, is(not(expect)));
    }

}
