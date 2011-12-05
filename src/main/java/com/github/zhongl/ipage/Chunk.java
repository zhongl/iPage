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
 * <li>{@link com.github.zhongl.ipage.Chunk} = {@link Record}* </li>
 * <li>{@link Record} = length:4bytes bytes</li>
 * </ul>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
final class Chunk implements Closeable, Iterable<Record> {

    static final int DEFAULT_CAPACITY = 4096; // 4k
    private final File file;
    private final long capacity;
    private final long beginPositionInIPage;

    private volatile MappedByteBuffer mappedByteBuffer;
    private volatile int writePosition = 0;

    Chunk(long beginPositionInIPage, File file, long capacity) throws IOException {
        this.beginPositionInIPage = beginPositionInIPage;
        this.file = file;
        this.capacity = capacity;
        this.writePosition = (int) file.length();
    }

    public long append(Record record) throws IOException {
        checkOverFlowIfAppend(record.length());
        long iPageOffset = writePosition + beginPositionInIPage;
        ensureMap();
        mappedByteBuffer.position(writePosition);
        writePosition += record.writeTo(mappedByteBuffer.duplicate());
        return iPageOffset;
    }

    public Record get(long offset) throws IOException {
        ensureMap();
        int newPosition = (int) (offset - beginPositionInIPage);
        try {
            ByteBuffer duplicate = mappedByteBuffer.duplicate();
            duplicate.position(newPosition);
            duplicate.limit(writePosition);
            return Record.readFrom(duplicate); // buffer to avoid modification of mappedDirectBuffer .
        } catch (RuntimeException e) {
            /**
             * include {@link IllegalArgumentException}, {@link java.nio.BufferUnderflowException},
             */
            throw new IllegalArgumentException("Can't get record with nvalid offset " + offset);
        }
    }

    @Override
    public void close() throws IOException {
        if (mappedByteBuffer != null) {
            flush();
            DirectByteBufferCleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
            trim();
        }
    }

    public long endPositionInIPage() {
        return beginPositionInIPage + writePosition - 1;
    }

    public long beginPositionInIPage() {
        return beginPositionInIPage;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Chunk");
        sb.append("{file=").append(file);
        sb.append(", capacity=").append(capacity);
        sb.append(", beginPositionInIPage=").append(beginPositionInIPage);
        sb.append(", writePosition=").append(writePosition);
        sb.append('}');
        return sb.toString();
    }

    public void flush() throws IOException {
        if (mappedByteBuffer == null) return;
        mappedByteBuffer.force();
    }

    @Override
    public Iterator<Record> iterator() {
        return new RecordIterator(writePosition);
    }

    public void erase() throws IOException {
        close();
        checkState(file.delete(), "Can't delete file %s ", file);
    }

    public Chunk truncate(long offset) throws IOException {
        long length = endPositionInIPage() - offset;
        File remains = new File(file.getParentFile(), offset + "");
        InputSupplier<InputStream> source = ByteStreams.slice(Files.newInputStreamSupplier(file), offset, length);
        Files.copy(source, remains);
        erase();
        return new Chunk(offset, remains, length);
    }

    private void checkOverFlowIfAppend(int length) {
        int appendedPosition = writePosition + length + Record.LENGTH_BYTES;
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

    public void recover() throws IOException {
        ensureMap();
        int offset = 0;
        while (offset < writePosition) {
            try {
                Record record = get(offset);
                offset += Record.LENGTH_BYTES + record.length();
            } catch (RuntimeException e) { // read a broken record
                writePosition = offset;
            }
        }
    }

    private class RecordIterator implements Iterator<Record> {
        private int offset;
        private final int limit;

        private RecordIterator(int limit) {this.limit = limit;}


        @Override
        public boolean hasNext() {
            return offset < limit;
        }

        @Override
        public Record next() {
            try {
                Record record = get(offset);
                offset += Record.LENGTH_BYTES + record.length();
                return record;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
