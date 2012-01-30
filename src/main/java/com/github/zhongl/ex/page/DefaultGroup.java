package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ChannelWriter;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class DefaultGroup implements Group {

    private final List<InnerCursor<?>> list;
    private final File file;
    private final Codec codec;

    private boolean notWrote = true;

    public DefaultGroup(File file, Codec codec) {
        this.file = file;
        this.codec = codec;
        list = new ArrayList<InnerCursor<?>>();
    }

    @Override
    public <T> Cursor<T> append(final T object) {
        checkNotNull(object);
        checkState(notWrote);
        InnerCursor<T> cursor = new InnerCursor<T>(object);
        this.list.add(cursor);
        return cursor;
    }

    public void writeTo(FileChannel channel, boolean force) throws IOException {
        notWrote = false;
        ByteBuffer[] buffers = new ByteBuffer[list.size()];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = list.get(i).encodeObject();
        }
        ChannelWriter.getInstance().write(channel, buffers, force);
    }

    private class InnerCursor<T> implements Cursor<T> {
        private final int objectHashCode;
        private final String objectToString;

        private T object;
        private Getter<T> getter;

        public InnerCursor(T object) {
            this.object = object;
            this.objectHashCode = object.hashCode();
            this.objectToString = object.toString();
        }

        @Override
        public int hashCode() {
            return objectHashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InnerCursor that = (InnerCursor) o;
            return objectToString.equals(that.objectToString);
        }

        @Override
        public String toString() {
            return getClass().getName() + " -> " + objectToString;
        }

        @Override
        public T get() {
            checkState(file.exists());
            if (object != null) return object;
            return getter.get();
        }

        ByteBuffer encodeObject() throws IOException {
            checkNotNull(object);
            ByteBuffer buffer = codec.encode(object);
            long position = FileChannels.getOrOpen(file).position();
            getter = new Getter<T>(position);
            object = null;
            return buffer;
        }

    }

    private class Getter<T> {

        private final long position;

        public Getter(long position) {
            this.position = position;
        }

        public T get() {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file);
            buffer.position((int) position);
            return codec.decode(buffer);
        }
    }
}
