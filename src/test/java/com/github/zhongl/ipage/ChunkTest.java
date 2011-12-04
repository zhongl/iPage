package com.github.zhongl.ipage;

import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Iterator;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ChunkTest extends FileBase {

    private Chunk chunk;

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

        chunk = new Chunk(0L, file, 4096L);
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

        chunk = new Chunk(0L, file, 4096L);
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

        chunk = new Chunk(0L, file, 4096L);
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

        chunk = new Chunk(0L, file, 4096L);
        chunk.recover();
        Iterator<Record> iterator = chunk.iterator();
        assertThat(iterator.next(), is(new Record(bytes0)));
        assertThat(iterator.hasNext(), is(false));
    }

}
