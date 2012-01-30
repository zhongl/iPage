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
class DefaultPage implements Page {

    private final File file;
    private final int capacity;
    private final Codec codec;
    private final long begin;

    private boolean opened;

    DefaultPage(File file, int capacity, Codec codec) {
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

        ((DefaultGroup) group).writeTo(channel, force);
    }

    @Override
    public void delete() {
        close();
        if (file.exists()) checkState(file.delete());
    }

    @Override
    public Group newGroup() {
        checkState(opened);
        return new DefaultGroup(file, codec);
    }

    @Override
    public void close() {
        FileChannels.closeChannelOf(file);
    }


}
