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

import com.github.zhongl.buffer.MappedDirectBuffers;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.util.List;

import static com.github.zhongl.buffer.CommonAccessors.STRING;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyChunkTest extends ChunkBase {

    @Test
    public void splitCase1() throws Exception {
        dir = testDir("splitCase1");
        newChunk();

        List<? extends Chunk<String>> chunks = chunk.split(32L, 64L);
        assertThat(chunks.size(), is(2));
        assertThat(new File(dir, "0").length(), is(32L));
        assertThat(new File(dir, "64").length(), is(4032L));

        close(chunks);
    }

    @Test
    public void splitCase2() throws Exception {
        dir = testDir("splitCase2");
        newChunk();

        List<? extends Chunk<String>> chunks = chunk.split(0L, 64L);
        assertThat(chunks.size(), is(1));
        assertNotExistFile("0");
        assertThat(new File(dir, "64").length(), is(4032L));

        close(chunks);
    }

    @Test
    public void splitCase3() throws Exception {
        dir = testDir("splitCase3");
        newChunk();

        List<? extends Chunk<String>> chunks = chunk.split(16L, 30L);
        assertThat(chunks.get(0), is(chunk));
    }

    @Test
    public void leftCase1() throws Exception {
        dir = testDir("leftCase1");
        newChunk();

        Chunk<String> left = chunk.left(16L);
        assertThat(left, is(not(chunk)));
        chunk = left; // let chunk be close when tear down
        assertThat(new File(dir, "0").length(), is(16L));
    }

    @Test
    public void leftCase2() throws Exception {
        dir = testDir("leftCase2");
        newChunk();

        chunk = chunk.left(0L);
        assertThat(chunk, is(nullValue()));
        assertNotExistFile("0");
    }

    @Test
    public void rightCase1() throws Exception {
        dir = testDir("rightCase1");
        newChunk();

        chunk = chunk.right(64L);
        assertThat(chunk, is(notNullValue()));
        assertNotExistFile("0");
        assertThat(new File(dir, "64").length(), is(4032L));
    }

    @Test
    public void rightCase1WithNonZeroBeginPosition() throws Exception {
        dir = testDir("rightCase1WithNonZeroBeginPosition");
        newChunk(4096);

        chunk = chunk.right(4128L);
        assertThat(chunk, is(notNullValue()));
        assertNotExistFile("4096");
        assertThat(new File(dir, "4128").length(), is(4064L));
    }

    @Test
    public void rightCase2() throws Exception {
        dir = testDir("rightCase2");
        newChunk();

        Chunk<String> right = chunk.right(0L);
        assertThat(right, is(chunk));
        assertThat(new File(dir, "0").length(), is(4096L));
    }

    @Test
    public void endPosition() throws Exception {
        dir = testDir("endPosition");
        newChunk();
        assertThat(chunk.endPosition(), is(4096 - 1L));
    }

    @Test(expected = ReadOnlyBufferException.class)
    public void append() throws Exception {
        dir = testDir("append");
        newChunk();
        chunk.append("");
    }

    private void close(List<? extends Chunk<String>> chunks) throws IOException {
        for (Chunk<String> c : chunks) c.close();
    }

    protected void newChunk() throws IOException {
        newChunk(0);
    }

    private void newChunk(int beginPosition) throws IOException {
        dir.mkdirs();
        FileOperator operator = FileOperator.writeable(new File(dir, beginPosition + ""), 4096);
        chunk = new AppendableChunk(new MappedDirectBuffers(), operator, STRING);
        fullFill(chunk);
        chunk = chunk.asReadOnly();
    }
}
