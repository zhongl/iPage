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
import com.github.zhongl.buffer.MappedDirectBuffer;
import com.github.zhongl.buffer.MappedDirectBuffers;
import com.github.zhongl.integerity.ValidateOrRecover;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ReadOnlyBufferException;
import java.util.List;

/**
 * {@link com.github.zhongl.ipage.Chunk}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
abstract class Chunk<T> implements Closeable, ValidateOrRecover<T, IOException> {
    protected final MappedDirectBuffers buffers;
    protected final FileOperator fileOperator;
    protected final Accessor<T> accessor;

    protected Chunk(MappedDirectBuffers buffers, FileOperator fileOperator, Accessor<T> accessor) throws IOException {
        this.buffers = buffers;
        this.fileOperator = fileOperator;
        this.accessor = accessor;
    }

    public abstract long append(T object) throws ReadOnlyBufferException, BufferOverflowException, IOException;

    /** Caution: a invalid offset may not be detected, you should validate by your self. */
    public T get(long offset) throws IOException {
        try {
            int localOffset = (int) (offset - beginPosition());
            return mappedDirectBuffer().readBy(accessor, localOffset);
        } catch (BufferUnderflowException e) { // invalid offset
        } catch (IllegalArgumentException e) {} // invalid offset
        return null;
    }

    public void flush() throws IOException { mappedDirectBuffer().flush(); }

    public abstract long endPosition();

    public abstract Chunk<T> asReadOnly() throws IOException;

    public long beginPosition() { return fileOperator.beginPosition(); }

    public void delete() { fileOperator.delete(); }

    public Cursor<T> next(Cursor<T> cursor) throws IOException {
        T value = get(cursor.offset());
        if (value == null) return cursor.tail();
        return cursor.forword(accessor.byteLengthOf(value), value);
    }

    /**
     * There are three split cases:
     * <p/>
     * <pre>
     * Case 1: split to three pieces and keep left and right.
     *         begin                 end
     *    |@@@@@@|--------------------|@@@@@@@@|
     *
     * Case 2: split to two pieces and keep right
     *  begin                   end
     *    |----------------------|@@@@@@@@@@@@@|
     *
     * Case 3: too small interval to split
     *  begin end
     *    |@@@@|@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@|
     *
     * </pre>
     */
    public abstract List<? extends Chunk<T>> split(long begin, long end) throws IOException;

    /**
     * There are throe left cases:
     * <pre>
     * Case 1: keep left and abandon right.
     *         offset
     *    |@@@@@@|-----------------------------|
     *
     * Case 2: abandon all
     *  offset
     *    |------------------------------------|
     *
     * </pre>
     */
    public abstract Chunk<T> left(long offset) throws IOException;


    /**
     * There are two right cases:
     * <pre>
     * Case 1: keep right and abandon left.
     *         offset
     *    |------|@@@@@@@@@@@@@@@@@@@@@@@@@@@@@|
     *
     * Case 2: keep all because offset is begin or too small interval
     *  offset
     *    |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@|
     * </pre>
     */
    public abstract Chunk<T> right(long offset) throws IOException;

    public abstract long length();

    protected MappedDirectBuffer mappedDirectBuffer() throws IOException {
        return buffers.getOrMapBy(fileOperator);
    }
}
