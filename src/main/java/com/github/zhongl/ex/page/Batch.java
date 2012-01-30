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
import com.github.zhongl.ex.nio.ChannelWriter;

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
    protected final Codec codec;
    protected final File file;

    private boolean notWrote = true;

    public Batch(Codec codec, File file) {
        this.codec = codec;
        this.file = file;
    }

    public <T> Cursor<T> append(final T object) {
        checkNotNull(object);
        checkState(notWrote);
        return createCursor(object);
    }

    void writeTo(FileChannel channel, boolean force) throws IOException {
        notWrote = false;
        ChannelWriter.getInstance().write(channel, getBuffers(), force);
    }

    protected abstract <T> Cursor<T> createCursor(T object);

    protected abstract ByteBuffer[] getBuffers() throws IOException;
}
