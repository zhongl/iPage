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
    private final Codec codec;

    private boolean opened;
    private Batch currentBatch;

    protected Page(File file, Number number, Codec codec) {
        super(number);
        createIfNotExist(file);
        this.file = file;
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

    public void force() {
        final FileChannel channel = FileChannels.getOrOpen(file);
        currentBatch = newBatch(this, (int) file().length(), currentBatch.writeAndForceTo(channel));
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
    public ByteBuffer encode(final Object value) { return codec().encode(value); }

    protected abstract boolean isOverflow();

    protected abstract Batch newBatch(Kit kit, int position, int estimateBufferSize);

    private void createIfNotExist(File file) { file.getParentFile().mkdirs(); }
}
