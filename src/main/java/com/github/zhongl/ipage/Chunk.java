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
        checkState(!erased, "Chunk has already erased.");
        try {
            return getInternal((int) (offset - beginPositionInIPage));
        } catch (RuntimeException e) {
            // include IllegalArgumentException java.nio.BufferUnderflowException,
            throw new IllegalArgumentException("Can't get object with invalid offset " + offset);
        }
    }

    @Override
    public void close() throws IOException {
        if (flush() && releaseBuffer()) setLength(writePosition);
    }

    public long endPositionInIPage() {
        checkState(!erased, "Chunk has already erased.");
        return beginPositionInIPage + writePosition - 1;
    }

    public long beginPositionInIPage() {
        checkState(!erased, "Chunk has already erased.");
        return beginPositionInIPage;
    }

    public boolean flush() throws IOException {
        if (erased || mappedByteBuffer == null) return false;
        mappedByteBuffer.force();
        return true;
    }

    @Override
    public Iterator<T> iterator() {
        checkState(!erased, "Chunk has already erased.");
        return new RecordIterator(writePosition);
    }

    public void erase() throws IOException {
        if (erased) return;
        erased = true;
        releaseBuffer();
        setLength(0L);
        deleteFile();
    }

    public Dimidiation dimidiate(long offset) {
        checkState(!erased, "Chunk has already erased.");
        return new Dimidiation((offset));
    }

    public long findOffsetOfFirstInvalidRecordBy(Validator<T> validator) throws IOException {
        long offset = beginPositionInIPage;
        while (true) {
            T object = get(offset);
            if (!validator.validate(object)) break;
            offset += byteBufferAccessor.lengthOf(object);
        }
        return offset;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Chunk");
        sb.append("{file=").append(file);
        sb.append(", capacity=").append(capacity);
        sb.append(", byteBufferAccessor=").append(byteBufferAccessor.getClass().getName());
        sb.append(", beginPositionInIPage=").append(beginPositionInIPage);
        sb.append(", writePosition=").append(writePosition);
        sb.append(", erased=").append(erased);
        sb.append('}');
        return sb.toString();
    }

    private T getInternal(int offset) throws IOException {
        ensureMap();
        ByteBuffer duplicate = mappedByteBuffer.duplicate();// avoid modification of mappedDirectBuffer.
        duplicate.position(offset);
        duplicate.limit(writePosition);
        return byteBufferAccessor.read(duplicate).get();
    }

    private void deleteFile() {
        File deleted = new File(file.getParentFile(), "-" + file.getName() + "-");
        checkState(file.renameTo(deleted), "Can't delete file %s", file); // void deleted failed
        checkState(deleted.delete(), "Can't delete file %s", file);
    }

    private void setLength(long value) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(value);
        randomAccessFile.close();
    }

    private boolean releaseBuffer() throws IOException {
        if (mappedByteBuffer == null) return false;
        DirectByteBufferCleaner.clean(mappedByteBuffer);
        mappedByteBuffer = null;
        return true;
    }

    private void checkOverFlowIfAppend(T record) {
        int appendedPosition = writePosition + byteBufferAccessor.lengthOf(record);
        if (appendedPosition > capacity) throw new OverflowException();
    }

    private void ensureMap() throws IOException {
        // TODO maybe a closed state is needed for prevent remap
        if (mappedByteBuffer == null) mappedByteBuffer = Files.map(file, READ_WRITE, capacity);
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
                // TODO Slove concurrent modification problem
                checkState(!erased, "Chunk has already erased.");
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
        private final long offset;

        public Dimidiation(long offset) {
            this.offset = offset;
        }

        public Chunk<T> left() throws IOException {
            writePosition = (int) (offset - beginPositionInIPage());
            return Chunk.this;
        }

        public Chunk<T> right() throws IOException {
            long length = endPositionInIPage() - offset + 1;
            File rightPiece = new File(file.getParentFile(), offset + "");
            InputSupplier<InputStream> source = ByteStreams.slice(Files.newInputStreamSupplier(file), offset, length);
            Files.copy(source, rightPiece);
            erase();
            return new Chunk<T>(offset, rightPiece, length, byteBufferAccessor);
        }
    }
}
