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

import com.github.zhongl.accessor.CommonAccessors;
import com.github.zhongl.integerity.Validator;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class AppendableChunkTest extends ChunkBase {

    @Test
    public void appendAndGet() throws Exception {
        file = testFile("appendAndGet");
        newChunk();

        String record = "record";
        long offset = chunk.append(record);
        assertThat(offset, is(0L));
        assertThat(chunk.get(offset), is(record));
    }

    @Test
    public void nextCursor() throws Exception {
        file = testFile("nextCursor");
        newChunk();

        for (int i = 0; i < 10; i++) {
            chunk.append("" + i);
        }

        Cursor<String> cursor = Cursor.begin(chunk.beginPosition());
        for (int i = 0; i < 10; i++) {
            cursor = chunk.next(cursor);
            assertThat(cursor.lastValue(), is(i + ""));
        }
    }

    @Test
    public void validate() throws Exception {
        file = testFile("checkCRC");
        newChunk();

        for (int i = 0; i < 10; i++) {
            chunk.append("" + i);
        }

        Validator<String, IOException> validator = new Validator<String, IOException>() {
            @Override
            public boolean validate(String value) throws IOException {
                return !value.equals(7 + "");
            }
        };

        assertThat(chunk.validateOrRecoverBy(validator), is(false));
    }

    @Test
    public void getByInvalidOffset() throws Exception {
        file = testFile("getByInvalidOffset");
        newChunk();
        chunk.append("record");
        assertThat(chunk.get(1L), is(nullValue()));
    }

    @Test
    public void split() throws Exception {
        file = testFile("split");
        newChunk();
        assertThat(chunk.split(0L, 7L).isEmpty(), is(true));
    }

    @Test
    public void left() throws Exception {
        file = testFile("left");
        newChunk();
        Chunk<String> left = chunk.left(7L);
        assertThat(left, is(chunk));
    }

    @Test
    public void right() throws Exception {
        file = testFile("right");
        newChunk();
        Chunk<String> right = chunk.right(7L);
        assertThat(right, is(chunk));
    }

    @Test
    public void endPosition() throws Exception {
        file = testFile("endPosition");
        newChunk();
        assertThat(chunk.endPosition(), is(chunk.beginPosition()));
        chunk.append("1234");
        assertThat(chunk.endPosition(), is(chunk.beginPosition() + 8 - 1));
    }

    protected void newChunk() throws IOException {
        long beginPositionInIPage = 0L;
        int capacity = 4096;
        chunk = Chunk.appendableChunk(file, beginPositionInIPage, capacity, CommonAccessors.STRING);
    }
}
