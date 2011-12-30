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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessors;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;

import java.util.LinkedList;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class SequenceTest extends FileBase {

    Sequence<String> sequence;

    @Test
    public void main() throws Exception {
        dir = testDir("main");
        LinkedList<LinkedPage<String>> linkedPages = new LinkedPageLoader<String>(dir, Accessors.STRING, 16).load();
        sequence = new Sequence<String>(linkedPages);
        String record = "record";
        Cursor cursor = sequence.append(record);
        assertThat(cursor, is(new Cursor(0L)));
        assertThat(sequence.get(cursor), is(record));
        assertThat(sequence.next(cursor), is(new Cursor(10L)));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        sequence.close();
        super.tearDown();
    }
}
