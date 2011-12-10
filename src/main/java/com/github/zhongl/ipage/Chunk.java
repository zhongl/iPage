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

import com.github.zhongl.accessor.Accessor;
import com.github.zhongl.integerity.ValidateOrRecover;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ReadOnlyBufferException;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * {@link com.github.zhongl.ipage.Chunk}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public abstract class Chunk<T> implements ValidateOrRecover<T, IOException> {
    public static final int DEFAULT_CAPACITY = 4096; // 4k

    public static <T> Chunk<T> asReadOnly(Chunk<T> chunk, int minimizeCollectLength) throws IOException {
        if (chunk instanceof ReadOnlyChunk) return chunk;
        chunk.close();
        return readOnlyChunk(chunk.file, chunk.beginPosition(), (int) chunk.file.length(), chunk.accessor, minimizeCollectLength);
    }

    public static <T> Chunk<T> readOnlyChunk(File file, long beginPosition, int capacity, Accessor<T> accessor, int minimizeCollectLength) throws IOException {
        return new ReadOnlyChunk<T>(file, beginPosition, capacity, accessor, minimizeCollectLength);
    }

    public static <T> Chunk<T> appendableChunk(File file, long beginPosition, int capcity, Accessor<T> accessor) throws IOException {
        return new AppendableChunk(file, beginPosition, capcity, accessor);
    }

    protected final Accessor<T> accessor;
    protected final long beginPosition;
    protected final File file;
    protected final MappedBufferFile mappedBufferFile;

    protected Chunk(File file, long beginPosition, Accessor<T> accessor, int capacity, boolean readOnly) throws IOException {
        this.file = file;
        this.accessor = accessor;
        this.beginPosition = beginPosition;
        mappedBufferFile = new MappedBufferFile(file, capacity, readOnly);
    }

    public abstract long append(T object) throws ReadOnlyBufferException, BufferOverflowException;

    public final T get(long offset) {
        try {
            int localOffset = (int) (offset - beginPosition());
            int length = (int) (endPosition() - offset + 1);
            return mappedBufferFile.readBy(accessor, localOffset, length);
        } catch (RuntimeException e) { // java.nio.BufferUnderflowException, java.lang.IllegalArgumentException
            return null; // invalide offset
        }
    }

    public void flush() {
        mappedBufferFile.flush();
    }

    public abstract long endPosition();

    public final long beginPosition() { return beginPosition; }

    public final void delete() {
        File deleted = new File(file.getParentFile(), "-" + file.getName() + "-"); // rename to "-$name-"
        checkState(file.renameTo(deleted), "Can't delete file %s", file); // void deleted failed
        truncate(deleted, 0L);
        checkState(deleted.delete(), "Can't delete file %s", deleted);
    }

    public final Cursor<T> next(Cursor<T> cursor) throws IOException {
        if (cursor.offset() >= endPosition()) return cursor.end();
        long offset = cursor.offset() < beginPosition() ? beginPosition() : cursor.offset();
        T value = get(offset);
        offset += accessor.byteLengthOf(value);
        return Cursor.cursor(offset, value);
    }

    public abstract void close();

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
    public abstract List<Chunk<T>> split(long begin, long end) throws IOException;


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

    protected static final void truncate(File file, long size) {
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(size);
            randomAccessFile.close();
        } catch (IOException e) {
            throw new IllegalStateException("Can't truncate file " + file, e);
        }
    }
}
