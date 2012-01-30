package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.FileChannels;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class FileChannelPage implements Page {

    private final File file;
    private final int capacity;
    private final Codec codec;
    private final long begin;

    private volatile boolean opened;

    FileChannelPage(File file, int capacity, Codec codec) {
        this.file = file;
        this.begin = Long.parseLong(file.getName());
        this.capacity = capacity;
        this.codec = codec;
        FileChannels.getOrOpen(file); // create file if not exist
        this.opened = true;
    }

    @Override
    public void commit(Group group, boolean force, OverflowCallback callback) throws IOException {
        checkState(opened);
        FileChannel channel = FileChannels.getOrOpen(file);

        if (channel.position() > capacity) {
            callback.onOverflow(group, force);
            return;
        }

        ((InnerGroup) group).writeTo(channel, force);
    }

    @Override
    public void delete() {
        close();
        if (file.exists()) checkState(file.delete());
    }

    @Override
    public Group newGroup() {
        checkState(opened);
        return new InnerGroup();
    }

    @Override
    public void close() {
        FileChannels.closeChannelOf(file);
    }

    @NotThreadSafe
    private class InnerGroup implements Group {

        @Override
        public <T> Cursor<T> append(final T object) {
            return new Cursor<T>() {
                @Override
                public T get() {
                    checkState(file.exists());
                    return object;
                }
            };
        }

        public void writeTo(FileChannel channel, boolean force) throws IOException {
            // TODO writeTo

            if (force) channel.force(false);
        }
    }

}
