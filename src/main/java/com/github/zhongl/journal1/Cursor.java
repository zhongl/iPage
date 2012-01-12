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

import com.google.common.primitives.Longs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Cursor {
    private static final int LENGTH_OF_POSITION = 8;
    private final ByteBuffer buffer;
    private final long position;

    public Cursor(ByteBuffer buffer, long position) {
        this.buffer = buffer;
        this.position = position;
    }

    public ByteBuffer get() throws IOException {
        return buffer;
    }

    public int writeTo(FileChannel channel) throws IOException {
        ByteBuffer checkpointBuffer = ByteBuffer.allocate(Page.FLAG_CRC32_LENGTH + LENGTH_OF_POSITION);
        CRC32 crc32 = new CRC32();
        crc32.update(Longs.toByteArray(position));
        checkpointBuffer.put(Page.SAVING);
        checkpointBuffer.putLong(crc32.getValue());
        checkpointBuffer.putInt(LENGTH_OF_POSITION);
        checkpointBuffer.putLong(position);
        checkpointBuffer.flip();
        return channel.write(checkpointBuffer);
    }
}
