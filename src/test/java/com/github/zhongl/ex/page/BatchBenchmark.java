package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.*;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BatchBenchmark extends FileTestContext implements Kit {

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
        codec = ComposedCodecBuilder.compose(new BytesCodec())
                                    .with(ChecksumCodec.class)
                                    .with(LengthCodec.class)
                                    .build();
        position = 0;
        valueLength = 4096;
        size = 10000;
    }


    @Test
    public void defaultBatch() throws Exception {
        file = testFile("defaultBatch");
        Batch batch = new DefaultBatch(this, position, valueLength);
        benchmark(batch);
    }

    @Test
    public void parallelEncodeBatch() throws Exception {
        file = testFile("parallelEncodeBatch");
        Batch batch = new ParallelEncodeBatch(this, position, valueLength);
        benchmark(batch);
    }

    private void benchmark(final Batch batch) throws IOException {
        final FileChannel channel = FileChannels.getOrOpen(file);

        Benchmarks.benchmark(batch.getClass().getSimpleName(), new Runnable() {
            @Override
            public void run() {

                for (int i = 0; i < size; i++) {
                    byte[] bytes = new byte[valueLength];
                    ByteBuffer.wrap(bytes).putInt(0, i);
                    batch.append(bytes);
                }

                batch.writeAndForceTo(channel);
            }
        }, size);

        FileChannels.closeChannelOf(file);
    }

    @Override
    public Cursor cursor(int offset) {
        return null;
    }

    @Override
    public ByteBuffer encode(Object value) {
        return codec.encode(value);
    }
}
