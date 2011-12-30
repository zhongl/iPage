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
import java.nio.ByteBuffer;
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
    private static final int LENGTH_BYTES = 4;

    private final File file;
    private final List<Event> list;
    private final WritableByteChannel channel;
    private final ChannelAccessor<Event> channelAccessor;

    public Page(File file, ChannelAccessor<Event> channelAccessor) throws IOException {
        this.file = file;
        this.channelAccessor = channelAccessor;
        this.list = tryLoadEventsFromExistFile();
        this.channel = new CRC32WritableByteChannel(file);
    }

    private List<Event> tryLoadEventsFromExistFile() throws IOException {
        if (!file.exists()) return new LinkedList<Event>();
        FileInputStream stream = new FileInputStream(file);
        try {
            long offset = file.length() - CRC32_LENGTH;
            checkState(validateCheckSum(stream, offset), "Invalid page file: %s", file);
            return loadEvents(stream, offset);
        } finally {
            stream.close();
        }
    }

    private List<Event> loadEvents(FileInputStream stream, long offset) throws IOException {
        FileChannel channel = stream.getChannel();
        LinkedList<Event> events = new LinkedList<Event>();
        ByteBuffer buffer = ByteBuffer.allocate(LENGTH_BYTES);
        while (channel.position() < offset) {
            buffer.rewind();
            checkState(channel.read(buffer) == LENGTH_BYTES, "Invalid event length.");
            buffer.flip();
            int length = buffer.getInt();
            Event event = channelAccessor.reader(length).readFrom(channel);
            events.add(event);
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
        list.add(event);
        ChannelAccessor.Writer writer = channelAccessor.writer(event);
        channel.write(lengthBuffer(writer.valueByteLength()));
        writer.writeTo(channel);
    }

    private static ByteBuffer lengthBuffer(int value) {
        return ByteBuffer.allocate(LENGTH_BYTES).putInt(0, value);
    }

    public void fix() throws IOException {
        channel.close();
    }

    @Override
    public Iterator<Event> iterator() {
        return list.iterator();
    }

    public void clear() {
        file.delete();
    }

}
