package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.*;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.util.FileTestContext;
import com.google.common.base.Stopwatch;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BatchBenchmark extends FileTestContext {

    private Page page = mock(Page.class);
    private int position;
    private int valueLength;
    private int size;
    private Codec codec;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        file = testFile("main");

        doReturn(file).when(page).file();
        doReturn(codec).when(page).codec();
        codec = ComposedCodecBuilder.compose(new ByteBufferCodec())
                                    .with(ChecksumCodec.class)
                                    .with(LengthCodec.class)
                                    .build();
        position = 0;
        valueLength = 4096;
        size = 1000;
    }


    @Test
    public void defaultBatch() throws Exception {
        file = testFile("defaultBatch");
        Batch batch = new DefaultBatch(new Factory(), position, valueLength);
        benchmark(batch);
    }

    @Test
    public void parallelEncodeBatch() throws Exception {
        file = testFile("parallelEncodeBatch");
        Batch batch = new ParallelEncodeBatch(new Factory(), position, valueLength);
        benchmark(batch);
    }

    private void benchmark(Batch batch) throws IOException {
        FileChannel channel = FileChannels.getOrOpen(file);

        Stopwatch stopwatch = new Stopwatch().start();
        for (int i = 0; i < size; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(new byte[valueLength]);
            buffer.putInt(0, i);
            batch.append(buffer);
        }

        batch.writeAndForceTo(channel);
        stopwatch.stop();
        System.out.println(batch.getClass().getSimpleName() + " : " + stopwatch);
        FileChannels.closeChannelOf(file);
    }

    class Factory implements CursorFactory {

        @Override
        public <T> Cursor<T> reader(final int offset) {
            return new Reader<T>(page, offset);
        }

        @Override
        public <T> ObjectRef<T> objectRef(final T object) {
            return new ObjectRef<T>(object, codec);
        }

        @Override
        public <T> Transformer<T> transformer(final Cursor<T> intiCursor) {
            return new Transformer<T>(intiCursor);
        }
    }


}
