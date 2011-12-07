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
import com.github.zhongl.util.FileBase;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
    public void iterate() throws Exception {
        file = testFile("iterate");
        newChunk();

        for (int i = 0; i < 10; i++) {
            chunk.append("" + i);
        }

        Iterator<String> iterator = chunk.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            assertThat(iterator.next(), is(i + ""));
        }

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

        assertThat(chunk.findOffsetOfFirstInvalidRecordBy(validator), is(35L));
    }

    @Test
    public void chunkToString() throws Exception {
        file = testFile("chunkToString");
        newChunk();
        String expect = "Chunk{file=target/tmpTestFiles/ChunkTest.chunkToString, " +
                "capacity=4096, " +
                "accessor=com.github.zhongl.accessor.CommonAccessors$StringAccessor, " +
                "beginPositionInIPage=0, " +
                "writePosition=0, " +
                "erased=false}";
        assertThat(chunk.toString(), is(expect));
    }

    @Test(expected = NoSuchElementException.class)
    public void iterateAfterErase() throws Exception {
        file = testFile("iterateAfterErase");
        newChunk();
        for (int i = 0; i < 10; i++) {
            chunk.append("" + i);
        }
        Iterator<String> iterator = chunk.iterator();
        iterator.next();

        chunk.erase();
        iterator.next();
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

    @Test(expected = IllegalArgumentException.class)
    public void getByInvalidOffset() throws Exception {
        file = testFile("getByInvalidOffset");
        newChunk();
        chunk.append("record");
        chunk.get(1L);
    }

    @Test
    public void closeAfterErase() throws Exception {
        file = testFile("closeAfterErase");
        newChunk();
        chunk.erase();
        chunk.close();
    }

    @Test
    public void eraseAfterErase() throws Exception {
        file = testFile("eraseAfterErase");
        newChunk();
        chunk.erase();
        chunk.erase();
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
        file = testFile("cleanAfterClose");
        newChunk();
        chunk.close();
        chunk.flush();
    }

    @Test
    public void closeAfterClose() throws Exception {
        file = testFile("cleanAfterClose");
        newChunk();
        chunk.close();
        chunk.close();
    }

    private void newChunk() throws IOException {
        long beginPositionInIPage = 0L;
        int capacity = 4096;
        chunk = new Chunk(beginPositionInIPage, file, capacity, CommonAccessors.STRING);
    }
}
