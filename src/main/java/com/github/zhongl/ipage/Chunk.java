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
import com.github.zhongl.integerity.Validator;
import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import static com.github.zhongl.util.ByteBuffers.slice;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link com.github.zhongl.ipage.Chunk}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class Chunk<T> implements Closeable, ValidateOrRecover<T, IOException> {

    public static final int DEFAULT_CAPACITY = 4096; // 4k
    private final Accessor<T> accessor;
    private final boolean readOnly;
    private final long beginPositionInIPage;
    private final File file;
    private final long capacity;

    private volatile int writePosition = 0;
    private volatile boolean erased;
    private volatile MappedByteBuffer mappedByteBuffer;

    public Chunk(long beginPositionInIPage, File file, long capacity, Accessor<T> accessor) throws IOException {
        this(beginPositionInIPage, file, capacity, accessor, false);
    }

    private Chunk(long beginPositionInIPage, File file, long capacity, Accessor<T> accessor, boolean readOnly) throws IOException {
        this.file = file;
        this.capacity = capacity;
        this.beginPositionInIPage = beginPositionInIPage;
        this.accessor = accessor;
        this.readOnly = readOnly;
        this.writePosition = (int) file.length();
    }

    public long append(T record) throws IOException, BufferOverflowException {
        if (readOnly) throw new UnsupportedOperationException("Readonly chunk.");
        checkState(!erased, "Chunk %s has already erased", file);
        checkOverFlowIfAppend(record);
        long iPageOffset = writePosition + beginPositionInIPage;
        ensureMap();
        writePosition += accessor.write(record, slice(mappedByteBuffer, writePosition));  // TODO use a better name to instead "slice"
        return iPageOffset;
    }

    public T get(long offset) throws IOException {
        checkState(!erased, "Chunk %s has already erased", file);
        return getInternal((int) (offset - beginPositionInIPage));
    }

    public boolean flush() {
        if (readOnly || erased || mappedByteBuffer == null) return false;
        mappedByteBuffer.force();
        return true;
    }

    public long endPositionInIPage() {
        checkState(!erased, "Chunk %s has already erased", file);
        return beginPositionInIPage + writePosition - 1;
    }

    public long beginPositionInIPage() {
        checkState(!erased, "Chunk %s has already erased", file);
        return beginPositionInIPage;
    }

    public void erase() throws IOException {
        if (erased) return;
        erased = true;
        releaseBuffer();
        deleteFile();
    }

    @Override
    public void close() throws IOException {
        if (flush() && releaseBuffer()) setLength(writePosition);
    }

    public Chunk<T> asReadOnly() throws IOException {
        close();
        return new Chunk<T>(beginPositionInIPage, file, writePosition, accessor, true);
    }

    @Override
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        Cursor<T> cursor = Cursor.begin(beginPositionInIPage);
        while (cursor.offset() < endPositionInIPage()) {
            long lastOffset = cursor.offset();
            cursor = next(cursor);
            if (validator.validate(cursor.lastValue())) continue;
            writePosition = (int) (lastOffset - beginPositionInIPage);
            return false;
        }
        return true;
    }

    public Cursor<T> next(Cursor<T> cursor) throws IOException {
        checkState(!erased, "Chunk %s has already erased", file);
        if (cursor.offset() >= endPositionInIPage()) return cursor.end();
        long offset = cursor.offset() < beginPositionInIPage ? beginPositionInIPage : cursor.offset();
        T value = get(offset);
        offset += accessor.byteLengthOf(value);
        return Cursor.cursor(offset, value);
    }


    int copyTo(WritableByteChannel channel, long begin, long end) throws IOException {
        if (erased) return 0;
        ensureMap();
        int offset = (int) (begin - beginPositionInIPage);
        int length = (int) (end - begin);
        if (length > writePosition) length -= writePosition;
        return channel.write(slice(mappedByteBuffer, offset, length));
    }

    private T getInternal(int offset) throws IOException {
        ensureMap();
        try {
            return accessor.read(slice(mappedByteBuffer, offset, writePosition - offset));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private void checkOverFlowIfAppend(T record) throws BufferOverflowException {
        int appendedPosition = writePosition + accessor.byteLengthOf(record);
        if (appendedPosition > capacity) throw new BufferOverflowException();
    }

    private boolean releaseBuffer() {
        if (mappedByteBuffer == null) return false;
        DirectByteBufferCleaner.clean(mappedByteBuffer);
        mappedByteBuffer = null;
        return true;
    }

    private void ensureMap() throws IOException {
        if (mappedByteBuffer != null) return;
        FileChannel.MapMode mode = readOnly ? READ_ONLY : READ_WRITE;
        mappedByteBuffer = Files.map(file, mode, capacity);
    }

    private void deleteFile() throws IOException {
        setLength(0L);
        File deleted = new File(file.getParentFile(), "-" + file.getName() + "-");
        checkState(file.renameTo(deleted), "Can't delete file %s", file); // void deleted failed
        checkState(deleted.delete(), "Can't delete file %s", file);
    }

    private void setLength(long value) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(value);
        randomAccessFile.close();
    }

}
