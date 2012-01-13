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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Event {

    static final byte SAVING = (byte) 0;
    static final byte APPEND = (byte) 1;

    // flag(1) CRC32(8) length(4) bytes
    static final int FLAG_CRC32_LENGTH = 1 + 8 + 4;
    private final ByteBuffer buffer;

    public static Event append(ByteBuffer event) {
        return new Event(wrap(APPEND, event));  // TODO append
    }

    public static Event saveCheckpoint(Cursor cursor) {
        byte[] bytes = Longs.toByteArray(cursor.position());
        return new Event(wrap(SAVING, ByteBuffer.wrap(bytes)));
    }

    private Event(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int writeTo(FileChannel channel) throws IOException {
        return channel.write(buffer);
    }

    private static ByteBuffer wrap(byte flag, ByteBuffer buffer) {
        ByteBuffer wrapped = ByteBuffer.wrap(new byte[FLAG_CRC32_LENGTH + buffer.limit()]);
        wrapped.put(flag);
        wrapped.putLong(crc32(buffer.duplicate()));
        wrapped.putInt(buffer.limit());
        wrapped.put(buffer.duplicate());
        wrapped.flip();
        return wrapped;
    }

    private static long crc32(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        if (buffer.isDirect()) {
            while (buffer.hasRemaining()) crc32.update(buffer.get());
        } else {
            crc32.update(buffer.array());
        }
        return crc32.getValue();
    }
}
