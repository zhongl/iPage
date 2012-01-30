package com.github.zhongl.ex.nio;

import com.github.zhongl.util.FileAsserter;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedChannelWriterTest extends FileTestContext {

    @Test
    public void write() throws Exception {
        file = testFile("write");

        FileChannel channel = new RAFileChannel(file);
        ByteBuffer[] buffers = new ByteBuffer[] {
                ByteBuffer.wrap(new byte[] {1}),
                ByteBuffer.wrap(new byte[] {2})
        };

        try {
            new MappedChannelWriter().write(channel, buffers, false);
        } finally {
            channel.close();
        }

        FileAsserter.assertExist(file).contentIs(new byte[] {1, 2});
    }
}
