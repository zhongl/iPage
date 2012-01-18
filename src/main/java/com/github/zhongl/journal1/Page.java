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

package com.github.zhongl.journal1;

import com.google.common.primitives.Longs;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class Page implements Closeable, Flushable {
    static final byte APPEND = (byte) 1;
    static final byte SAVING = (byte) 0;

    static final int READ_BUFFER_SIZE = Integer.getInteger("ipage.read.buffer.size", 4096);

    private final File file;
    private final int capacity;
    private final long number;

    private volatile FileChannel channel;
    private volatile int writePosition;
    private volatile int readPosition;
    private volatile boolean readonly;
    private volatile Event head;

    public Page(File file, int capacity) {
        this.file = file;
        this.capacity = capacity;
        this.number = Long.parseLong(this.file.getName());
        this.readonly = file.exists() && file.length() >= capacity;
        this.writePosition = (int) file.length();
        this.readPosition = 0;
    }

    public Page append(ByteBuffer buffer) throws IOException {
        return append(new Event(APPEND, buffer));
    }

    public Page saveCheckpoint(long position) throws IOException {
        return append(new Event(SAVING, ByteBuffer.wrap(Longs.toByteArray(position))));
    }

    public Cursor head() throws IOException {
        if (readPosition == writePosition) throw new EOFException();
        if (head == null) head = readPacket(false);
        if (head.type() == SAVING) return new Cursor(ByteBuffer.wrap(new byte[0]), number + readPosition);
        if (head.type() == APPEND) return new Cursor(head.body(), number + readPosition);
        throw new IllegalStateException("Unknown page type.");
    }

    public Page remove() throws IOException {
        if (readPosition == writePosition) throw new EOFException();

        if (head == null) head = readPacket(false);
        readPosition += head.length() + Event.FLAG_CRC32_LENGTH;
        head = null;

        if (readPosition < writePosition) return this;

        if (readPosition == writePosition) {
            return readonly ? deleteAndGetNextPage() : this;
        }

        throw new IllegalStateException("Forwarded read position should not greater than file length");
    }

    private Page append(Event event) throws IOException {
        if (readonly) throw new IllegalStateException("Can't append to readonly page.");
        // TODO throw IllegalStateException if buffer size greater than capacity.
        writePosition += channel().write(event.toBuffer());
        return writePosition < capacity ? this : fixedAndGetNewPage();
    }

    private FileChannel channel() throws FileNotFoundException {
        if (channel == null)
            channel = new RandomAccessFile(file, readonly ? "r" : "rw").getChannel();
        return channel;
    }

    private Page fixedAndGetNewPage() throws IOException {
        close();
        channel = null;
        readonly = true;
        return new Page(nextFile(), capacity);
    }

    private File nextFile() {
        return new File(file.getParentFile(), number + writePosition + "");
    }

    private Event readPacket(boolean validate) throws IOException {
        for (int i = 1; ; i++) {
            channel().position(readPosition);
            ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE * i);
            channel().read(buffer);
            buffer.flip();
            try {
                return Event.readFrom(buffer, validate);
            } catch (IllegalArgumentException ignored) {
                // TODO auto resize allocating
            }
        }
    }

    private Page deleteAndGetNextPage() throws IOException {
        delete();
        return new Page(nextFile(), capacity);
    }

    public void delete() throws IOException {
        close();
        file.delete();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Page page = (Page) o;
        return !(file != null ? !file.equals(page.file) : page.file != null);
    }

    @Override
    public int hashCode() {
        return file != null ? file.hashCode() : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Page");
        sb.append("{file=").append(file);
        sb.append(", writePosition=").append(writePosition);
        sb.append(", readPosition=").append(readPosition);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void flush() throws IOException {
        if (channel != null) channel.force(false);
    }

    @Override
    public void close() throws IOException {
        if (channel == null) return;
        channel.close();
        channel = null;
    }

    public long recoverAndGetLastCheckpoint() throws IOException {
        long lastCheckpoint = -1L;
        while (readPosition < writePosition) {
            try {
                Event event = readPacket(true);
                if (event.type() == SAVING) lastCheckpoint = event.body().getLong(0);
                readPosition += Event.FLAG_CRC32_LENGTH + event.length();
            } catch (Exception e) {
                channel.truncate(readPosition);
                break;
            }
        }
        readPosition = 0;
        return lastCheckpoint;
    }

    public long number() {
        return number;
    }

    public void setHead(long position) {
        readPosition = (int) (position - number());
    }

    public int compareTo(long position) {
        if (position < number()) return 1;
        if (position >= number() + writePosition) return -1;
        return 0;
    }
}
