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

package com.github.zhongl.page;

import com.github.zhongl.nio.FileChannels;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Page<T> implements Comparable<Cursor>, Closeable, Flushable {
    private static final int FOUR_BYTES = 4;

    private final File file;
    private final long begin;
    private final int capacity;
    private final Recorder<T> recorder;
    private final ByteBuffer length = ByteBuffer.allocate(FOUR_BYTES);

    private volatile int position;
    private FileChannel channel;

    Page(File file, int capacity, Recorder<T> recorder) {
        this.file = file;
        this.begin = Long.parseLong(file.getName());
        this.capacity = capacity;
        this.recorder = recorder;
        position = 0;
    }

    public Cursor append(T record) throws OverflowException, IOException {
        Recorder.Writer writer = recorder.writer(record);
        int recordLength = writer.valueByteLength();

        if (position + recordLength > capacity) throw new OverflowException();

        Cursor cursor = new Cursor(begin + position);
        channel().write(length.putInt(0, recordLength)); // put length
        writer.writeTo(channel()); // put record
        position += recordLength + FOUR_BYTES;
        return cursor;
    }

    private FileChannel channel() throws IOException {
        if (channel == null) channel = FileChannels.channel(this.file, this.capacity);
        return channel;
    }

    public Page<T> multiply() {
        return null;  // TODO multiply
    }

    public T get(Cursor cursor) throws IOException {
        int offset = (int) (cursor.offset - begin);
        int recordLength = getRecordLength(offset);
        return recorder.reader(recordLength).readFrom(channel());
    }

    private int getRecordLength(int offset) throws IOException {
        channel().position(offset);
        length.rewind();
        channel().read(length);
        length.flip();
        return length.getInt();
    }

    public T remove(Cursor cursor) {
        return null;  // TODO remove
    }

    public Cursor next(Cursor cursor) {
        return null;  // TODO next
    }

    @Override
    public int compareTo(Cursor cursor) {
        if (cursor.offset < begin) return 1;
        if (cursor.offset > position) return -1;
        return 0;
    }

    private long end() {
        return position;
    }

    @Override
    public void close() throws IOException {
        channel().truncate(position);
        channel().close();
    }

    @Override
    public void flush() throws IOException {
        channel().force(true);
    }

    public void tryRecover() {
        // TODO tryRecover
    }
}
