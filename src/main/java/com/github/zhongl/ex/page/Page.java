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

/**
 * {@link com.github.zhongl.ex.page.Page} is a high level abstract entity focus on IO manipulation.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public abstract class Page implements Closable {

    private final File file;
    private final long number;
    private final int capacity;
    private final Codec codec;

    private boolean opened;
    private Batch currentBatch;

    protected Page(File file, long number, int capacity, Codec codec) {
        this.file = file;
        this.number = number;
        this.capacity = capacity;
        this.codec = codec;
        this.opened = true;
    }

    public <T> Cursor<T> append(T value, boolean force, OverflowCallback<T> callback) throws IOException {
        checkState(opened);

        FileChannel channel = FileChannels.getOrOpen(file);

        if (channel.position() > capacity) return callback.onOverflow(value, force);
        if (currentBatch == null) currentBatch = newBatch(file, (int) channel.size(), codec, 0);

        Cursor<T> cursor = currentBatch.append(value);

        if (force) {
            currentBatch.writeAndForceTo(channel);
            currentBatch = newBatch(file, (int) channel.size(), codec, currentBatch.estimateBufferSize);
        }

        return cursor;
    }

    public File file() {
        return file;
    }

    public long number() {
        return number;
    }

    @Override
    public void close() {
        if (!opened) return;
        opened = false;
        FileChannels.closeChannelOf(file);
    }

    protected abstract Batch newBatch(File file, int position, Codec codec, int estimateBufferSize);
}
