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

package com.github.zhongl.ex.journal;

import com.github.zhongl.ex.codec.ByteBuffers;
import com.github.zhongl.ex.nio.MappedByteBuffers;
import com.github.zhongl.ex.nio.MappingOptions;
import com.google.common.io.Closeables;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedBufferPage implements Page {

    private final MappingOptions options;

    private volatile int length;

    public MappedBufferPage(MappingOptions options) {
        this.options = options;
        length = options.file().exists() ? (int) options.file().length() : 0;
    }

    @Override
    public long offset() {
        return Long.parseLong(options.file().getName());
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public int append(ByteBuffer buffer, boolean force, OverflowCallback callback) {
        int delta = ByteBuffers.lengthOf(buffer);
        MappedByteBuffer byteBuffer = MappedByteBuffers.getOrMapBy(options);
        byteBuffer.position(length);
        if (byteBuffer.remaining() < delta)
            return callback.onOverflow(buffer, force);
        byteBuffer.put(buffer);
        if (force) byteBuffer.force();
        return length += delta;
    }

    @Override
    public ByteBuffer slice(int offset, int length) {
        ByteBuffer buffer = MappedByteBuffers.getOrMapBy(options).asReadOnlyBuffer();
        buffer.position(offset);
        buffer.limit(offset + length);
        return buffer;
    }

    @Override
    public void delete() {
        close();
        options.file().delete();
    }

    @Override
    public void close() {
        if (options.mode() == READ_WRITE) {
            MappedByteBuffers.getOrMapBy(options).force();
            try { truncate(); } catch (FileNotFoundException ignored) { }
        }
        MappedByteBuffers.clear(options);
    }

    private void truncate() throws FileNotFoundException {
        FileOutputStream stream = new FileOutputStream(options.file());
        try {
            stream.getChannel().truncate(length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            Closeables.closeQuietly(stream);
        }
    }

    @Override
    public int compareTo(Long o) {
        if (o < offset()) return 1;
        if (o >= offset() + length()) return -1;
        return 0;
    }

}
