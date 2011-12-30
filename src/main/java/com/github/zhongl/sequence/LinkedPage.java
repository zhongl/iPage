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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;

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
        this(file, accessor, (int) file.length(), readOnlyChannels);
        page.fix();
    }

    public Cursor append(T object) throws OverflowException, IOException {
        Accessor.Writer writer = accessor.writer(object);

        if (position + writer.valueByteLength() > capacity) throw new OverflowException();

        Cursor cursor = new Cursor(begin + position);
        position += page.add(object);
        return cursor;
    }

    public LinkedPage<T> multiply() throws IOException {
        File newFile = new File(file.getParentFile(), position + "");
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
        if (cursor.offset < begin) return 1;
        if (cursor.offset >= position) return -1;
        return 0;
    }

    @Override
    public void close() throws IOException {
        page.fix();
        readOnlyChannels.close(file);
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
