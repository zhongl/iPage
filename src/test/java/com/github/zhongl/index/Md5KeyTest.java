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

package com.github.zhongl.index;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Md5KeyTest {

    @Test(expected = IllegalArgumentException.class)
    public void invalidInitialBytesLessThan16() throws Exception {
        new Md5Key(new byte[15]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidInitialBytesGreaterThan16() throws Exception {
        new Md5Key(new byte[17]);
    }

    @Test
    public void md5KeyToString() throws Exception {
        String expect = "Md5Key{md5Bytes=1bc29b36f623ba82aaf6724fd3b16718}";
        assertThat(Md5Key.generate("md5".getBytes()).toString(), is(expect));
    }
}
