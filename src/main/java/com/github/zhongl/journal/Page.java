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

package com.github.zhongl.journal;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Page implements Iterable<Event> {
    private static final int CRC32_LENGTH = 8;

    private final File file;
    private final List<Event> list;
    private final WritableByteChannel channel;
    private final Accessor<Event> accessor;

    Page(File file, Accessor<Event> accessor) throws IOException {
        this.file = file;
        this.accessor = accessor;
        this.list = tryLoadEventsFromExistFile();
        this.channel = list.isEmpty() ? new CRC32WritableByteChannel(file) : null;
    }

    private List<Event> tryLoadEventsFromExistFile() throws IOException {
        if (!file.exists()) return new LinkedList<Event>();
        FileInputStream stream = new FileInputStream(file);
        try {
            long offset = file.length() - CRC32_LENGTH;
            checkState(validateCheckSum(stream, offset), "Invalid page %s", file);
            return loadEvents(stream, offset);
        } finally {
            stream.close();
        }
    }

    private List<Event> loadEvents(FileInputStream stream, long offset) throws IOException {
        FileChannel channel = stream.getChannel();
        LinkedList<Event> events = new LinkedList<Event>();
        channel.position(0L);
        while (channel.position() < offset) {
            events.add(accessor.reader().readFrom(channel));
        }
        return events;
    }

    private static boolean validateCheckSum(FileInputStream fileInputStream, long offset) throws IOException {
        DataInputStream stream = new DataInputStream(new BufferedInputStream(fileInputStream));
        CRC32 crc32 = new CRC32();
        for (long i = 0; i < offset; i++) {
            crc32.update(stream.read());
        }
        return crc32.getValue() == stream.readLong();
    }

    public void add(Event event) throws IOException {
        checkState(channel != null && channel.isOpen(), "Fixed page can't add event");
        accessor.writer(event).writeTo(channel);
        list.add(event);
    }

    public void fix() throws IOException {
        channel.close();
    }

    @Override
    public Iterator<Event> iterator() {
        return list.iterator();
    }

    public void clear() {
        checkState(file.delete(), "Can't delete page %s", file);
    }

}
