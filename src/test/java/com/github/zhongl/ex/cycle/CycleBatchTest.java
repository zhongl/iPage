package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.page.DefaultCursor;
import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Closeables;
import org.junit.Test;

import java.nio.channels.FileChannel;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CycleBatchTest extends FileTestContext {
    @Test
    public void alignLength() throws Exception {
        file = testFile("alignLength");

        CycleBatch batch = new CycleBatch(new StringCodec(), 0L, 4, 0);
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
