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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Closeables.closeQuietly;

/**
 * {@link com.github.zhongl.ex.page.Page} is a high level abstract entity focus on IO manipulation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public abstract class Page extends Numbered implements Closable, CursorFactory {

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
    }

    public <T> Cursor append(T value, boolean force, OverflowCallback callback) throws IOException {
        checkState(opened);

        final FileChannel channel = FileChannels.getOrOpen(file);
        final int size = (int) channel.size();

        if (checkOverflow(size, capacity)) {
            currentBatch = newBatch(this, size, currentBatch.writeAndForceTo(channel));
            return callback.onOverflow(value, force);
        }

        if (currentBatch == null) currentBatch = newBatch(this, size, 0);
        final Cursor cursor = currentBatch.append(value);

        if (force) currentBatch = newBatch(this, size, currentBatch.writeAndForceTo(channel));
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
    public Reader reader(final int offset) {
        if (file().length() == 0) return null;
        return new Reader(Page.this, offset);
    }

    @Override
    public ObjectRef objectRef(final Object object) {
        return new ObjectRef(object, codec);
    }

    @Override
    public Proxy proxy(final Cursor intiCursor) {
        return new Proxy(intiCursor);
    }

    protected boolean checkOverflow(int size, int capacity) { return size > capacity; }

    protected abstract Batch newBatch(CursorFactory cursorFactory, int position, int estimateBufferSize);

    private void createIfNotExist(File file) {
        if (file.exists()) return;
        file.getParentFile().mkdirs();
    }
}
