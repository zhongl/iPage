package com.github.zhongl.ex.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class ChannelWriter {

    public static final String CLASS_NAME = System.getProperty(
            "ipage.channel.writer.class.name",
            "com.github.zhongl.ex.nio.MappedChannelWriter");

    private static final ChannelWriter SINGLETON;

    static {
        try {
            SINGLETON = (ChannelWriter) Class.forName(CLASS_NAME).newInstance();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static ChannelWriter getInstance() {
        return SINGLETON;
    }

    public abstract void write(FileChannel channel, ByteBuffer[] buffers, boolean force) throws IOException;
}
