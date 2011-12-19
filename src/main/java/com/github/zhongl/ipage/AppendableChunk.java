/*
 * Copyright 2011 zhongl
 *
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

package com.github.zhongl.ipage;

import com.github.zhongl.buffer.Accessor;
import com.github.zhongl.buffer.MappedDirectBuffers;
import com.github.zhongl.integrity.Validator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ReadOnlyBufferException;
import java.util.Collections;
import java.util.List;

/**
 * {@link AppendableChunk}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
class AppendableChunk<T> extends Chunk<T> {
    private volatile int writePosition = 0;

    public AppendableChunk(MappedDirectBuffers buffers, FileOperator fileOperator, Accessor<T> accessor) throws IOException {
        super(buffers, fileOperator, accessor);
    }

    @Override
    public long append(T object) throws ReadOnlyBufferException, BufferOverflowException, IOException {
        long iPageOffset = writePosition + beginPosition();
        writePosition += mappedDirectBuffer().writeBy(accessor, writePosition, object);
        return iPageOffset;
    }

    @Override
    public long endPosition() {
        if (writePosition == 0) return beginPosition();
        return beginPosition() + writePosition - 1;
    }

    @Override
    public Chunk<T> asReadOnly() throws IOException {
        close();
        return new ReadOnlyChunk<T>(buffers, fileOperator.asReadOnly(), accessor);
    }

    @Override
    public void close() throws IOException {
        flush();
        mappedDirectBuffer().release();
        fileOperator.left(writePosition);
    }

    @Override
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        Cursor<T> cursor = Cursor.<T>head().skipTo(beginPosition());
        while (cursor.offset() < endPosition()) {
            long lastOffset = cursor.offset();
            cursor = next(cursor);
            if (validator.validate(cursor.lastValue())) continue;
            writePosition = (int) (lastOffset - beginPosition());
            return false;
        }
        return true;
    }

    @Override
    public List<? extends Chunk<T>> split(long begin, long end) throws IOException {
        return Collections.singletonList(this); // unsupport for appending chunk
    }

    @Override
    public Chunk<T> left(long offset) throws IOException { return this; }/*unsupport for appending chunk*/

    @Override
    public Chunk<T> right(long offset) throws IOException { return this; }/*unsupport for appending chunk*/

    @Override
    public long length() { return writePosition; }

}
