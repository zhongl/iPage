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
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkTest extends FileBase {

    private Chunk<String> chunk;

    @Override
    public void tearDown() throws Exception {
        chunk.close();
        super.tearDown();
    }

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
    public void dimidiateAndGetLeft() throws Exception {
        file = testFile("dimidiateAndGetRight");
        newChunk();

        String left = "left";
        String right = "right";
        chunk.append(left);
        long offset = chunk.append(right);
        Chunk<String>.Dimidiation dimidiation = chunk.dimidiate(offset);
        assertThat(dimidiation.left().get(0L), is(left));
    }

    @Test
    public void dimidiateAndGetRight() throws Exception {
        file = testFile("dimidiateAndGetRight");
        newChunk();

        String left = "left";
        String right = "right";
        chunk.append(left);
        long offset = chunk.append(right);
        Chunk<String>.Dimidiation dimidiation = chunk.dimidiate(offset);
        assertThat(dimidiation.right().get(offset), is(right));
    }

    @Test(expected = IllegalStateException.class)
    public void appendAfterErase() throws Exception {
        file = testFile("appendAfterErase");
        newChunk();
        chunk.erase();
        chunk.append("Oops");
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterErase() throws Exception {
        file = testFile("getAfterErase");
        newChunk();
        chunk.erase();
        chunk.get(0L);
    }

    @Test(expected = IllegalStateException.class)
    public void dimidiateAfterErase() throws Exception {
        file = testFile("dimidiateAfterErase");
        newChunk();
        chunk.erase();
        chunk.dimidiate(0L);
    }

    @Test(expected = IllegalStateException.class)
    public void iteratorAfterErase() throws Exception {
        file = testFile("iteratorAfterErase");
        newChunk();
        chunk.erase();
        chunk.iterator();
    }

    @Test(expected = IllegalStateException.class)
    public void getBeginPositionAfterErase() throws Exception {
        file = testFile("getBeginPositionAfterErase");
        newChunk();
        chunk.erase();
        chunk.beginPositionInIPage();
    }

    @Test(expected = IllegalStateException.class)
    public void getEndPositionAfterErase() throws Exception {
        file = testFile("getEndPositionAfterErase");
        newChunk();
        chunk.erase();
        chunk.endPositionInIPage();
    }

    @Test
    public void closeAfterErase() throws Exception {
        file = testFile("closeAfterErase");
        newChunk();
        chunk.erase();
        chunk.close();
    }

    @Test
    public void flushAfterErase() throws Exception {
        file = testFile("flushAfterErase");
        newChunk();
        chunk.erase();
        chunk.flush();
    }

    @Test
    public void flushAfterClose() throws Exception {
        file = testFile("flushAfterClose");
        newChunk();
        chunk.close();
        chunk.flush();
    }

    private void newChunk() throws IOException {
        long beginPositionInIPage = 0L;
        int capacity = 4096;
        ByteBufferAccessor<String> byteBufferAccessor = new StringAccessor();
        chunk = new Chunk(beginPositionInIPage, file, capacity, byteBufferAccessor);
    }
}
