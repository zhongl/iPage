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

    static final int PAGE_SIZE = Integer.getInteger("ipage.io.page.size", 4096);

    private final File file;
    private final int capacity;
    private final long number;

    private volatile FileChannel channel;
    private volatile int writePosition;
    private volatile int readPosition;
    private volatile boolean readonly;
    private volatile Packet head;


    public Page(File file, int capacity) {
        this.file = file;
        this.capacity = capacity;
        this.number = Long.parseLong(this.file.getName());
        this.readonly = file.exists() && file.length() >= capacity;
        this.writePosition = (int) file.length();
        this.readPosition = 0;
    }

    public Page append(ByteBuffer buffer) throws IOException {
        return append(new Packet(APPEND, buffer));
    }

    public Page saveCheckpoint(Cursor cursor) throws IOException {
        return append(new Packet(SAVING, ByteBuffer.wrap(Longs.toByteArray(cursor.position()))));
    }

    public Cursor head() throws IOException {
        if (readPosition == writePosition) throw new EOFException();
        if (head == null) head = readPacket();
        if (head.type() == SAVING) return new Cursor(ByteBuffer.wrap(new byte[0]), number + readPosition);
        if (head.type() == APPEND) return new Cursor(head.body(), number + readPosition);
        throw new IllegalStateException("Unknown page type.");
    }

    public Page remove() throws IOException {
        if (readPosition == writePosition) throw new EOFException();

        if (head == null) head = readPacket();
        readPosition += head.length() + Packet.FLAG_CRC32_LENGTH;
        head = null;

        if (readPosition < writePosition) return this;

        if (readPosition == writePosition) {
            return readonly ? deleteAndGetNextPage() : this;
        }

        throw new IllegalStateException("Forwarded read position should not greater than file length");
    }

    private Page append(Packet packet) throws IOException {
        if (readonly) throw new IllegalStateException("Can't append to readonly page.");
        // TODO throw IllegalStateException if buffer size greater than capacity.
        if (channel == null) channel = new RandomAccessFile(file, "rw").getChannel();
        writePosition += channel.write(packet.toBuffer());
        return writePosition < capacity ? this : fixedAndGetNewPage();
    }

    private Page fixedAndGetNewPage() throws IOException {
        channel.close();
        channel = null;
        readonly = true;
        return new Page(nextFile(), capacity);
    }

    private File nextFile() {
        return new File(file.getParentFile(), number + writePosition + "");
    }

    private Packet readPacket() throws IOException {
        if (channel == null) channel = new FileInputStream(file).getChannel();
        for (int i = 1; ; i++) {
            channel.position(readPosition);
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE * i);
            channel.read(buffer);
            buffer.flip();
            try {
                return Packet.readFrom(buffer, false);
            } catch (IllegalArgumentException ignored) {
                // TODO auto resize allocating
            }
        }
    }

    private Page deleteAndGetNextPage() throws IOException {
        close();
        file.delete();
        return new Page(nextFile(), capacity);
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

}
