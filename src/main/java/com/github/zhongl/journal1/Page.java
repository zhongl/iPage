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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Page implements Closeable, Flushable {
    static final byte APPEND = (byte) 1;
    static final byte SAVING = (byte) 0;

    // flag(1) CRC32(8) length(4) bytes
    static final int FLAG_CRC32_LENGTH = 1 + 8 + 4;
    static final int PAGE_SIZE = Integer.getInteger("ipage.io.page.size", 4096);

    private final File file;
    private final int capacity;
    private final long number;

    private volatile FileChannel channel;
    private volatile int writePosition;
    private volatile int readPosition;
    private volatile boolean readonly;


    public Page(File file, int capacity) {
        this.file = file;
        this.capacity = capacity;
        this.number = Long.parseLong(this.file.getName());

        this.readonly = file.exists() && file.length() >= capacity;
        this.writePosition = (int) file.length();
        this.readPosition = 0;
    }

    public Page append(ByteBuffer event) throws IOException {
        if (readonly) throw new IllegalStateException("Can't append to readonly page.");
        // TODO throw IllegalStateException if event size greater than capacity.
        if (channel == null) channel = new RandomAccessFile(file, "rw").getChannel();

        writePosition += channel.write(wrap(event));

        if (writePosition < capacity) return this;

        channel.close();
        channel = null;
        readonly = true;
        return new Page(nextFile(), capacity);
    }

    private ByteBuffer wrap(ByteBuffer buffer) {
        ByteBuffer wrapped = ByteBuffer.wrap(new byte[FLAG_CRC32_LENGTH + buffer.limit()]);
        wrapped.put(APPEND);
        wrapped.putLong(crc32(buffer.duplicate()));
        wrapped.putInt(buffer.limit());
        wrapped.put(buffer.duplicate());
        wrapped.flip();
        return wrapped;
    }

    private long crc32(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        if (buffer.isDirect()) {
            while (buffer.hasRemaining()) crc32.update(buffer.get());
        } else {
            crc32.update(buffer.array());
        }
        return crc32.getValue();
    }

    private File nextFile() {
        return new File(file.getParentFile(), number + writePosition + "");
    }

    public Cursor head() throws IOException {
        if (readPosition == writePosition) throw new EOFException();
        if (channel == null) channel = new FileInputStream(file).getChannel();

        channel.position(readPosition);
        ByteBuffer buffer = read(PAGE_SIZE);
        byte flag = buffer.get();
        if (flag == SAVING) return new Cursor(ByteBuffer.wrap(new byte[0]), number + readPosition);
        if (flag == APPEND) return new Cursor(get(buffer), number + readPosition);
        throw new IllegalStateException("Unknown page flag.");
    }

    private ByteBuffer read(int size) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(size);
        channel.read(buffer);
        buffer.flip();
        return buffer;
    }

    public Page remove() throws IOException {
        if (readPosition == writePosition) throw new EOFException();

        if (channel == null) channel = new FileInputStream(file).getChannel();
        channel.position(readPosition);
        ByteBuffer buffer = read(PAGE_SIZE);
        buffer.get(); // skip flag
        buffer.getLong(); // skip crc32
        readPosition += buffer.getInt() + FLAG_CRC32_LENGTH;

        if (readPosition < writePosition) return this;

        if (readPosition == writePosition) {
            if (!readonly) return this;
            close();
            file.delete();
            return new Page(nextFile(), capacity);
        }

        throw new IllegalStateException("Forwarded read position should not greater than file length");
    }

    public Page saveCheckpoint(Cursor cursor) throws IOException {
        if (channel == null) channel = new RandomAccessFile(file, "rw").getChannel();

        writePosition += cursor.writeTo(channel);
        if (writePosition < capacity) return this;

        channel.close();
        channel = null;
        readonly = true;
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

    private ByteBuffer get(ByteBuffer buffer) throws IOException {
        buffer.getLong(); // skip crc32
        int length = buffer.getInt();
        if (length <= buffer.remaining()) {
            buffer.limit(buffer.position() + length);
            return buffer.slice();
        }
        if (length > buffer.remaining() && buffer.limit() == buffer.capacity()) {
            int more = length - buffer.remaining();
            ByteBuffer result = ByteBuffer.allocate(length);
            result.put(buffer).put(read(more));
            return result;
        }
        throw new IllegalStateException("Invalid length while reading page.");
    }

}
