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

import com.github.zhongl.kvengine.Record;
import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkRecoveryTest extends FileBase {

    private Chunk chunk;
    private ByteBufferAccessor accessor;

    @Override
    public void tearDown() throws Exception {
        chunk.close();
        super.tearDown();
    }

    @Test
    public void recoverWithoutRemove() throws Exception {
        file = testFile("recoverWithoutRemove");
        byte[] bytes0 = "record0".getBytes();
        byte[] bytes1 = "record1".getBytes();
        byte[] chunkContents = ChunkContentUtils.concatToChunkContentWith(bytes0, bytes1);
        Files.write(chunkContents, file); // mock chunk content

        chunk = new Chunk(0L, file, 4096L, accessor);
        chunk.recover();
        Iterator<Record> iterator = chunk.iterator();
        assertThat(iterator.next(), is(new Record(bytes0)));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(new Record(bytes1)));
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void recoverWithRemoveBecauseOfBufferUnderflow() throws Exception {
        file = testFile("recoverWithRemoveBecauseOfBufferUnderflow");
        byte[] bytes0 = "record0".getBytes();
        byte[] chunkContents = Bytes.concat(Ints.toByteArray(bytes0.length), bytes0, new byte[] {1});
        Files.write(chunkContents, file); // mock chunk content

        chunk = new Chunk(0L, file, 4096L, accessor);
        chunk.recover();
        Iterator<Record> iterator = chunk.iterator();
        assertThat(iterator.next(), is(new Record(bytes0)));
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void recoverWithRemoveBecauseOfInvalidLength() throws Exception {
        file = testFile("recoverWithRemoveBecauseOfInvalidLength");
        byte[] bytes0 = "record0".getBytes();
        byte[] chunkContents = Bytes.concat(Ints.toByteArray(bytes0.length), bytes0, Ints.toByteArray(-1));
        Files.write(chunkContents, file); // mock chunk content

        chunk = new Chunk(0L, file, 4096L, accessor);
        chunk.recover();
        Iterator<Record> iterator = chunk.iterator();
        assertThat(iterator.next(), is(new Record(bytes0)));
        assertThat(iterator.hasNext(), is(false));
    }

    @Test
    public void recoverWithRemoveBecauseOfHalfContent() throws Exception {
        file = testFile("recoverWithRemoveBecauseOfInvalidLength");
        byte[] bytes0 = "record0".getBytes();
        byte[] chunkContents = Bytes.concat(Ints.toByteArray(bytes0.length), bytes0, Ints.toByteArray(4), new byte[] {1});
        Files.write(chunkContents, file); // mock chunk content

        chunk = new Chunk(0L, file, 4096L, accessor);
        chunk.recover();
        Iterator<Record> iterator = chunk.iterator();
        assertThat(iterator.next(), is(new Record(bytes0)));
        assertThat(iterator.hasNext(), is(false));
    }

}
