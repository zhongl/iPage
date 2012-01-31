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

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BatchBenchmark extends FileTestContext {

    private Codec codec;
    private int position;
    private int valueLength;
    private int size;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
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
        Batch batch = new DefaultBatch(file, position, codec, valueLength);
        benchmark(batch);
    }

    @Test
    public void parallelEncodeBatch() throws Exception {
        file = testFile("parallelEncodeBatch");
        Batch batch = new ParallelEncodeBatch(file, position, codec, valueLength);
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

}
