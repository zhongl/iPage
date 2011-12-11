/*
 * Copyright 2011 zhongl
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

package com.github.zhongl.buffer;

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.nio.ReadOnlyBufferException;

import static com.github.zhongl.util.ByteBuffers.slice;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link MappedBufferFile}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class MappedBufferFile {

    private final MappedByteBuffer buffer;
    private volatile boolean released;

    public MappedBufferFile(File file, int capacity, boolean readOnly) throws IOException {
        this.buffer = Files.map(file, readOnly ? READ_ONLY : READ_WRITE, capacity);
    }

    public <T> int writeBy(Accessor<T> accessor, int offset, T object) throws ReadOnlyBufferException, BufferOverflowException {
        checkState(!released, "MappedDirectBuffer is not loaded.");
        int length = accessor.byteLengthOf(object);
        if (offset + length > buffer.limit()) throw new BufferOverflowException();
        return accessor.write(object, slice(buffer, offset, length));
    }

    public void flush() {
        if (!released) buffer.force();
    }

    public <T> T readBy(Accessor<T> accessor, int offset, int length) {
        checkState(!released, "MappedDirectBuffer is not loaded.");
        return accessor.read(slice(buffer, offset, length));
    }

    public void release() {
        if (!released) DirectByteBufferCleaner.clean(buffer);
        released = true;
    }


}
