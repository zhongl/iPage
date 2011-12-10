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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyChunkTest extends ChunkBase {

    @Test
    public void splitCase1() throws Exception {
        dir = testDir("splitCase1");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        List<Chunk<String>> chunks = chunk.split(32L, 64L);
        assertThat(chunks.size(), is(2));
        assertThat(new File(dir, "0").length(), is(32L));
        assertThat(new File(dir, "64").length(), is(4032L));

        close(chunks);
    }

    @Test
    public void splitCase2() throws Exception {
        dir = testDir("splitCase2");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        List<Chunk<String>> chunks = chunk.split(0L, 64L);
        assertThat(chunks.size(), is(1));
        assertNotExistFile("0");
        assertThat(new File(dir, "64").length(), is(4032L));

        close(chunks);
    }

    @Test
    public void splitCase3() throws Exception {
        dir = testDir("splitCase3");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        List<Chunk<String>> chunks = chunk.split(16L, 32L);
        assertThat(chunks.size(), is(0));
    }

    @Test
    public void leftCase1() throws Exception {
        dir = testDir("leftCase1");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        Chunk<String> left = chunk.left(16L);
        assertThat(left, is(not(chunk)));
        chunk = left; // let chunk be close when tear down
        assertThat(new File(dir, "0").length(), is(16L));
    }

    @Test
    public void leftCase2() throws Exception {
        dir = testDir("leftCase2");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        assertThat(chunk.left(0L), is(nullValue()));
        assertNotExistFile("0");
    }

    @Test
    public void rightCase1() throws Exception {
        dir = testDir("rightCase1");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        Chunk<String> right = chunk.right(64L);
        assertThat(right, is(notNullValue()));
        assertNotExistFile("0");
        assertThat(new File(dir, "64").length(), is(4032L));
    }

    @Test
    public void rightCase2() throws Exception {
        dir = testDir("rightCase2");
        dir.mkdirs();
        file = new File(dir, "0");
        newChunk();

        Chunk<String> right = chunk.right(0L);
        assertThat(right, is(chunk));
        assertThat(new File(dir, "0").length(), is(4096L));
    }

    @Test
    public void endPosition() throws Exception {
        file = testFile("endPosition");
        newChunk();
        assertThat(chunk.endPosition(), is(4096 - 1L));
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void append() throws Exception {
        file = testFile("append");
        newChunk();
        chunk.append("");
    }

    private void close(List<Chunk<String>> chunks) throws IOException {
        for (Chunk<String> c : chunks) {
            c.close();
        }
    }

    protected void newChunk() throws IOException {
        long beginPosition = 0L;
        int capacity = 4096;
        int minimizeCollectLength = 16;
        chunk = Chunk.appendableChunk(file, beginPosition, capacity, CommonAccessors.STRING);
        fullFill(chunk);
        chunk = Chunk.asReadOnly(chunk, minimizeCollectLength);
    }
}
