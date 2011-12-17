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
import com.github.zhongl.integerity.Validator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * {@link com.github.zhongl.ipage.ReadOnlyChunk}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
class ReadOnlyChunk<T> extends Chunk<T> {

    public ReadOnlyChunk(MappedDirectBuffers buffers, FileOperator fileOperator, Accessor<T> accessor) throws IOException {
        super(buffers, fileOperator, accessor);
    }

    @Override
    public long append(T object) throws ReadOnlyBufferException, BufferOverflowException {
        throw new ReadOnlyBufferException();
    }

    @Override
    public long endPosition() { return beginPosition() + fileOperator.length() - 1; }

    @Override
    public long length() { return fileOperator.length(); }

    @Override
    public Chunk<T> asReadOnly() { return this; }

    @Override
    public void close() throws IOException { mappedDirectBuffer().release(); }

    @Override
    @Deprecated
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        return true; // TODO remove this method
    }

    /** @see Chunk#split(long, long) */
    @Override
    public List<? extends Chunk<T>> split(long begin, long end) throws IOException {
        T value = get(begin);
        if (end - begin < accessor.byteLengthOf(value)) return singletonList(this);    // Case 3
        if (begin == beginPosition()) return singletonList(right(end));                // Case 2
        Chunk<T> right = right0(end); // do right first for avoiding delete by left
        Chunk<T> left = left(begin);
        return Arrays.asList(left, right);                                             // Case 1
    }

    /** @see Chunk#left(long) */
    @Override
    public Chunk<T> left(long offset) throws IOException {
        close();
        if (offset == beginPosition()) {                                               // Case 2
            delete();
            return null;
        }
        long size = offset - beginPosition();
        return new ReadOnlyChunk(buffers, fileOperator.left(size), accessor);          // Case 1
    }

    /** @see Chunk#right(long) */
    @Override
    public Chunk<T> right(long offset) throws IOException {
        if (offset == beginPosition()) return this;  // Case 2
        Chunk<T> chunk = right0(offset);
        delete();                                    // Case 1
        return chunk;
    }

    private Chunk<T> right0(long offset) throws IOException {
        return new ReadOnlyChunk(buffers, fileOperator.right(offset), accessor);
    }
}
