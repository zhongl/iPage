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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.Page;
import com.github.zhongl.page.ReadOnlyChannels;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Longs;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.CRC32;

import static java.util.Collections.singletonList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class LinkedPage<T> implements Comparable<Cursor>, Closeable {
    private static final int LENGTH_BYTES = 4;
    private final File file;
    private final Accessor<T> accessor;
    private final long begin;
    private final int capacity;
    private final InnerPage page;
    private final ReadOnlyChannels readOnlyChannels;

    private volatile int position;

    LinkedPage(File file, Accessor<T> accessor, int capacity, ReadOnlyChannels readOnlyChannels) throws IOException {
        this.file = file;
        this.accessor = accessor;
        this.capacity = capacity;
        this.readOnlyChannels = readOnlyChannels;
        page = new InnerPage(file, accessor);
        this.begin = Long.parseLong(file.getName());
        position = (int) file.length();
    }

    LinkedPage(File file, Accessor<T> accessor, ReadOnlyChannels readOnlyChannels) throws IOException {
        this(file, accessor, (int) file.length() - Page.CRC32_LENGTH, readOnlyChannels);
    }

    public Cursor append(T object) throws OverflowException, IOException {
        Accessor.Writer writer = accessor.writer(object);

        if (position + writer.valueByteLength() > capacity) throw new OverflowException();

        Cursor cursor = new Cursor(begin + position);
        position += page.add(object);
        return cursor;
    }

    public LinkedPage<T> multiply() throws IOException {
        File newFile = new File(file.getParentFile(), begin() + length() + "");
        LinkedPage<T> newPage = new LinkedPage<T>(newFile, accessor, capacity, readOnlyChannels);
        page.fix();
        return newPage;
    }

    public T get(Cursor cursor) throws IOException {
        int offset = (int) (cursor.offset - begin);
        FileChannel readOnlyChannel = readOnlyChannels.getOrCreateBy(file);
        readOnlyChannel.position(offset);
        return accessor.reader().readFrom(readOnlyChannel);
    }

    public Cursor next(Cursor cursor) throws IOException {
        return cursor.forword(accessor.writer(get(cursor)).valueByteLength() + LENGTH_BYTES);
    }

    @Override
    public int compareTo(Cursor cursor) {
        if (cursor.offset < begin()) return 1;
        if (cursor.offset >= begin() + length()) return -1;
        return 0;
    }

    @Override
    public void close() throws IOException {
        page.fix();
        readOnlyChannels.close(file);
    }

    public long begin() {
        return begin;
    }

    public long length() {
        return position;
    }

    public void clear() {
        page.clear();
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
    public List<LinkedPage<T>> split(Cursor begin, Cursor end) throws IOException {
        T value = get(begin);
        if (end.offset - begin.offset < accessor.writer(value).valueByteLength() + 4/* length bytes*/)
            return singletonList(this);                                         // Case 3
        if (begin.offset == begin()) return singletonList(right(end));          // Case 2
        LinkedPage<T> right = right0(end); // do right first for avoiding delete by left
        LinkedPage<T> left = left(begin);
        return Arrays.asList(left, right);                                       // Case 1
    }

    /**
     * There are two right cases:
     * <pre>
     * Case 1: keep right and abandon left.
     *         cursor
     *    |------|@@@@@@@@@@@@@@@@@@@@@@@@@@@@@|
     *
     * Case 2: keep all because cursor is begin or too small interval
     *  cursor
     *    |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@|
     * </pre>
     */
    public LinkedPage<T> right(Cursor cursor) throws IOException {
        if (cursor.offset == begin()) return this;        // Case 2
        LinkedPage<T> chunk = right0(cursor);
        clear();                                    // Case 1
        return chunk;
    }

    /**
     * There are throe left cases:
     * <pre>
     * Case 1: keep left and abandon right.
     *         cursor
     *    |@@@@@@|-----------------------------|
     *
     * Case 2: abandon all
     *  cursor
     *    |------------------------------------|
     *
     * </pre>
     */
    public LinkedPage<T> left(Cursor cursor) throws IOException {
        close();
        if (cursor.offset <= begin()) {                                               // Case 2
            clear();
            return null;
        }
        long size = cursor.offset - begin();

        InputSupplier<InputStream> supplier = ByteStreams.slice(Files.newInputStreamSupplier(file), 0L, size);
        long checksum = ByteStreams.getChecksum(supplier, new CRC32());

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(size);
        randomAccessFile.seek(size);
        randomAccessFile.writeLong(checksum);
        randomAccessFile.close();
        return new LinkedPage(file, accessor, readOnlyChannels);          // Case 1
    }

    private LinkedPage<T> right0(Cursor cursor) throws IOException {
        File newFile = new File(file.getParentFile(), Long.toString(cursor.offset));
        long offset = cursor.offset - begin();
        long length = length() - offset;

        InputSupplier<InputStream> from = ByteStreams.slice(Files.newInputStreamSupplier(file), offset, length);
        long checksum = ByteStreams.getChecksum(from, new CRC32());

        Files.copy(ByteStreams.join(from, ByteStreams.newInputStreamSupplier(Longs.toByteArray(checksum))), newFile);

        return new LinkedPage(newFile, accessor, readOnlyChannels);
    }

    private class InnerPage extends Page<T> {

        public InnerPage(File file, Accessor<T> accessor) throws IOException {
            super(file, accessor);
        }

        @Override
        public Iterator<T> iterator() {
            return null;
        }
    }
}
