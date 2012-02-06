/*
 * Copyright 2012 zhongl
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Closeables.closeQuietly;

/**
 * {@link com.github.zhongl.ex.page.Page} is a high level abstract entity focus on IO manipulation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public abstract class Page extends Numbered implements Closable, Kit {

    private final File file;
    private final int capacity;
    private final Codec codec;

    private boolean opened;
    private Batch currentBatch;

    protected Page(File file, Number number, int capacity, Codec codec) {
        super(number);
        createIfNotExist(file);
        this.file = file;
        this.capacity = capacity;
        this.codec = codec;
        this.opened = true;
        currentBatch = newBatch(this, (int) file.length(), 0);
    }

    public boolean append(Object value, FutureCallback<Cursor> forceCallback) {
        checkState(opened);
        if (isOverflow()) return false;
        currentBatch.append(value, forceCallback);
        return true;
    }

    protected abstract boolean isOverflow();

    public void force() {
        final FileChannel channel = FileChannels.getOrOpen(file);
        currentBatch = newBatch(this, (int) file().length(), currentBatch.writeAndForceTo(channel));
    }

    public <T> Cursor append(T value, boolean force, OverflowCallback callback) throws IOException {
        checkState(opened);

        final FileChannel channel = FileChannels.getOrOpen(file);
        final int size = (int) channel.size();

        if (checkOverflow(size, capacity)) {
            force(channel);
            return callback.onOverflow(value, force);
        }

        final Cursor cursor = currentBatch.append(value);

        if (force) force(channel);
        return cursor;
    }

    public Codec codec() {return codec;}

    public File file() { return file; }

    @Override
    public void close() {
        if (!opened) return;
        opened = false;
        closeQuietly(FileChannels.getOrOpen(file)); // ensure close file channel.
        FileChannels.closeChannelOf(file);
    }

    @Override
    public Cursor cursor(final int offset) {
        if (file().length() == 0) return null;
        return new Cursor() {
            @Override
            public <T> T get() {
                checkState(file().exists());
                ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file());
                buffer.position(offset);
                return codec().decode(buffer);
            }
        };
    }

    @Override
    public ByteBuffer encode(final Object value) {
        return codec().encode(value);
    }

    protected boolean checkOverflow(int size, int capacity) { return size > capacity; }

    protected final void force(FileChannel channel) throws IOException {
        currentBatch = newBatch(this, (int) channel.size(), currentBatch.writeAndForceTo(channel));
    }

    protected abstract Batch newBatch(Kit kit, int position, int estimateBufferSize);

    private void createIfNotExist(File file) { file.getParentFile().mkdirs(); }
}
