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

import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkIteratorTest extends FileBase {

    @Test
    public void iterate() throws Exception {
        dir = testDir("iterate");
        // create a iPage with two chunk
        IPage iPage = IPage.baseOn(dir).build();
        for (int i = 0; i < 513; i++) {
            iPage.append(new Record(Ints.toByteArray(i)));
        }


        assertExistFile("0");
        assertExistFile("4096");

        Iterator<Record> iterator = new ChunkIterator(dir);
        for (int i = 0; i < 513; i++) {
            assertThat(iterator.next(), is(new Record(Ints.toByteArray(i))));
        }

        assertThat(iterator.hasNext(), is(false));

        iPage.close();
    }

}
