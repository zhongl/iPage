/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.journal;

import com.github.zhongl.cache.Cache;
import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.CRC32WriteOnlyChannel;
import com.github.zhongl.page.Page;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class EventPage extends Page<Event> {
    private static final int CRC32_LENGTH = 8;

    private final List<Event> list;
    private boolean pageNotExisted;
    private final Cache cache;

    EventPage(File file, Accessor<Event> accessor, Cache cache) throws IOException {
        super(file, accessor);
        this.cache = cache;
        this.list = tryLoadFromExistFile();
    }

    @Override
    public int add(Event object) throws IOException {
        list.add(object);
        return super.add(object);
    }

    @Override
    public void clear() {
        for (Event event : list) cache.weak(event);
        list.clear();
        super.clear();
    }

    @Override
    public Iterator<Event> iterator() {
        return list.iterator();
    }

    private List<Event> tryLoadFromExistFile() throws IOException {
        if (pageNotExisted) return new LinkedList<Event>();

        FileInputStream stream = new FileInputStream(file);
        long offset = file.length() - CRC32_LENGTH;
        try {
            checkState(offset >= 0 && validateCheckSum(stream, offset));
            return load(stream, file.length() - CRC32_LENGTH);
        } finally {
            stream.close();
        }
    }

    private static boolean validateCheckSum(FileInputStream fileInputStream, long offset) throws IOException {
        DataInputStream stream = new DataInputStream(new BufferedInputStream(fileInputStream));
        CRC32 crc32 = new CRC32();
        for (long i = 0; i < offset; i++) {
            crc32.update(stream.read());
        }
        return crc32.getValue() == stream.readLong();
    }

    private List<Event> load(FileInputStream stream, long offset) throws IOException {
        FileChannel channel = stream.getChannel();
        LinkedList<Event> events = new LinkedList<Event>();
        channel.position(0L);
        while (channel.position() < offset) {
            events.add(accessor.reader().readFrom(channel));
        }
        return events;
    }

    protected WritableByteChannel createWriteOnlyChannel(File file) throws FileNotFoundException {
        pageNotExisted = true;
        return new CRC32WriteOnlyChannel(file);
    }
}
