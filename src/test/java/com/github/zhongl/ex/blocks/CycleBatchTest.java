package com.github.zhongl.ex.blocks;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.page.DefaultBatch;
import com.github.zhongl.ex.page.DefaultCursor;
import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.Tuple;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Closeables;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CycleBatchTest extends FileTestContext {
    @Test
    public void alignLength() throws Exception {
        file = testFile("alignLength");

        final Codec codec = new StringCodec();
        DefaultBatch batch = new DefaultBatch(codec, 0L / 4, 0) {
            private final int blockSize = 4;

            @Override
            protected Tuple aggregate(Tuple tuple, ByteBuffers.Aggregater aggregater) {
                ByteBuffer buffer = bufferIn(tuple);

                long offset = position;
                int bufferLength = ByteBuffers.lengthOf(buffer);
                int numOfBlocks = bufferLength / blockSize + 1;

                aggregater.concat(buffer, numOfBlocks * blockSize);

                position += numOfBlocks;

                return new Tuple(callbackIn(tuple), offset, numOfBlocks);
            }
        };
        CallbackFuture<Cursor> future0 = new CallbackFuture<Cursor>();
        CallbackFuture<Cursor> future1 = new CallbackFuture<Cursor>();

        batch.append("12345", future0);
        batch.append("12345", future1);

        FileChannel channel = FileChannels.getOrOpen(file);
        batch.writeAndForceTo(channel);
        assertThat(channel.size(), is(16L));

        assertThat((DefaultCursor) future0.get(), is(new DefaultCursor(0L, 2)));
        assertThat((DefaultCursor) future1.get(), is(new DefaultCursor(2L, 2)));

        Closeables.closeQuietly(channel);
    }
}
