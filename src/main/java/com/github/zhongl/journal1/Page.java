/*
 * Copyright 2012 zhongl
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

    private final File file;
    private final int capacity;
    private final boolean readonly;

    private volatile FileChannel channel;
    private volatile int writePosition;

    public Page(File file, int capacity) {
        this.file = file;
        this.capacity = capacity;
        this.readonly = file.exists() && file.length() >= capacity;
        this.writePosition = (int) file.length();
    }

    public Page append(ByteBuffer buffer) throws IOException {
        if (readonly) throw new IllegalStateException("Can't append to readonly page.");
        // TODO throw IllegalStateException if buffer size greater than capacity.
        if (channel == null) channel = new RandomAccessFile(file, "rw").getChannel();

        writePosition += channel.write(wrap(buffer));

        if (writePosition < capacity) return this;

        channel.close();
        channel = null;
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
        return new File(file.getParentFile(), Long.parseLong(file.getName()) + writePosition + "");
    }

    public Cursor head() {
        return null;  // TODO head
    }

    public Page remove() {
        return null;  // TODO remove 
    }

    public Page saveCheckpoint(Cursor cursor) {
        return null;  // TODO saveCheckpoint
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
