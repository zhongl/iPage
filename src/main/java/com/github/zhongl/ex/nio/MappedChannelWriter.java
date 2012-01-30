package com.github.zhongl.ex.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class MappedChannelWriter extends ChannelWriter {

    @Override
    public void write(FileChannel channel, ByteBuffer[] buffers, boolean force) throws IOException {
        int size = 0;

        for (ByteBuffer buffer : buffers) {
            size += ByteBuffers.lengthOf(buffer);
        }

        MappedByteBuffer mappedByteBuffer = channel.map(READ_WRITE, channel.size(), size);

        for (ByteBuffer buffer : buffers) {
            mappedByteBuffer.put(buffer);
        }

        mappedByteBuffer.force();
    }
}
