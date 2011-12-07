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

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link com.github.zhongl.ipage.Chunk} File structure :
 * <ul>
 * <p/>
 * <li>{@link com.github.zhongl.ipage.Chunk} = {@link T}* </li>
 * <li>{@link T} = length:4bytes bytes</li>
 * </ul>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
final class Chunk<T> implements Closeable, Iterable<T> {

    static final int DEFAULT_CAPACITY = 4096; // 4k
    private final File file;
    private final long capacity;
    private final ByteBufferAccessor<T> byteBufferAccessor;
    private final long beginPositionInIPage;

    private volatile MappedByteBuffer mappedByteBuffer;
    private volatile int writePosition = 0;
    private volatile boolean erased;

    Chunk(long beginPositionInIPage, File file, long capacity, ByteBufferAccessor<T> byteBufferAccessor) throws IOException {
        this.beginPositionInIPage = beginPositionInIPage;
        this.file = file;
        this.capacity = capacity;
        this.byteBufferAccessor = byteBufferAccessor;
        this.writePosition = (int) file.length();
    }

    public long append(T record) throws IOException {
        checkState(!erased, "Chunk has already erased.");
        checkOverFlowIfAppend(record);
        long iPageOffset = writePosition + beginPositionInIPage;
        ensureMap();
        ByteBuffer duplicate = mappedByteBuffer.duplicate();
        duplicate.position(writePosition);
        writePosition += byteBufferAccessor.write(record).to(duplicate);
        return iPageOffset;
    }

    public T get(long offset) throws IOException {
        try {
            return getInternal((int) (offset - beginPositionInIPage));
        } catch (RuntimeException e) {
            /**
             * include {@link IllegalArgumentException}, {@link java.nio.BufferUnderflowException},
             */
            throw new IllegalArgumentException("Can't get record with invalid offset " + offset);
        }
    }

    private T getInternal(int offset) throws IOException {
        checkState(!erased, "Chunk has already erased.");
        ensureMap();
        ByteBuffer duplicate = mappedByteBuffer.duplicate();// avoid modification of mappedDirectBuffer.
        duplicate.position(offset);
        duplicate.limit(writePosition);
        return byteBufferAccessor.read(duplicate).get();
    }

    @Override
    public void close() throws IOException {
        if (!erased && mappedByteBuffer != null) {
            flush();
            DirectByteBufferCleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
            trim();
        }
    }

    public long endPositionInIPage() {
        checkState(!erased, "Chunk has already erased.");
        return beginPositionInIPage + writePosition;
    }

    public long beginPositionInIPage() {
        checkState(!erased, "Chunk has already erased.");
        return beginPositionInIPage;
    }

    public void flush() throws IOException {
        if (erased || mappedByteBuffer == null) return;
        mappedByteBuffer.force();
    }

    @Override
    public Iterator<T> iterator() {
        checkState(!erased, "Chunk has already erased.");
        return new RecordIterator(writePosition);
    }

    public void erase() throws IOException {
        if (erased) return;
        close();
        checkState(file.delete(), "Can't delete file %s ", file);
        erased = true;
    }

    public Dimidiation dimidiate(long offset) {
        checkState(!erased, "Chunk has already erased.");
        return new Dimidiation((int) (offset - beginPositionInIPage));
    }

    @Deprecated
    public void recover() throws IOException {
        // TODO use dimidate
    }

    private void checkOverFlowIfAppend(T record) {
        int appendedPosition = writePosition + byteBufferAccessor.lengthOf(record);
        if (appendedPosition > capacity) throw new OverflowException();
    }

    private void ensureMap() throws IOException {
        // TODO maybe a closed state is needed for prevent remap
        if (mappedByteBuffer == null) mappedByteBuffer = Files.map(file, READ_WRITE, capacity);
    }

    private void trim() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(writePosition);
        randomAccessFile.close();
    }

    private class RecordIterator extends AbstractIterator<T> {
        private int offset;
        private final int limit;

        private RecordIterator(int limit) {
            this.limit = limit;
        }

        @Override
        protected T computeNext() {
            try {
                if (offset >= limit) return endOfData();
                T record = getInternal(offset);
                offset += byteBufferAccessor.lengthOf(record);
                return record;
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (RuntimeException e) { // read the appending chunk may cause the exception.
                return endOfData();
            }
        }
    }

    public class Dimidiation {
        private final int offset;

        public Dimidiation(int offset) {
            this.offset = offset;
        }

        public Chunk<T> left() throws IOException {
            writePosition = offset;
            return Chunk.this;
        }

        public Chunk<T> right() throws IOException {
            long length = endPositionInIPage() - offset;
            File remains = new File(file.getParentFile(), offset + "");
            InputSupplier<InputStream> source = ByteStreams.slice(Files.newInputStreamSupplier(file), offset, length);
            Files.copy(source, remains);
            erase();
            return new Chunk(offset, remains, length, byteBufferAccessor);
        }
    }
}
