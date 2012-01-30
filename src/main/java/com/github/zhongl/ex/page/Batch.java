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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class Batch {

    protected final File file;
    protected final int position;
    protected final Codec codec;
    protected ByteBuffer aggregatedBuffer;
    protected int estimateBufferSize;
    private boolean notWrote = true;

    public Batch(final File file, int position, final Codec codec, int estimateBufferSize) {
        this.file = file;
        this.position = position;
        this.codec = codec;
        this.estimateBufferSize = estimateBufferSize < 4096 ? 4096 : estimateBufferSize;
    }

    public <T> Cursor<T> append(final T object) {
        checkNotNull(object);
        checkState(notWrote);
        return _append(object);
    }

    void writeAndForceTo(FileChannel channel) throws IOException {
        notWrote = false;
        _writeAndForceTo(channel);
    }

    protected abstract <T> Cursor<T> _append(T object);

    protected abstract void _writeAndForceTo(FileChannel channel) throws IOException;
}
