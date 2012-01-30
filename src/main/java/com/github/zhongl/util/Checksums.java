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

package com.github.zhongl.util;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static com.github.zhongl.ex.nio.ByteBuffers.lengthOf;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Checksums {
    public static void validate(ByteBuffer buffer, long checksum) {
        checkState(checksum(buffer) == checksum, "Invalid checksum.");
    }

    public static long checksum(ByteBuffer buffer) {
        return checksum_bytes(buffer.duplicate());
    }

    static long checksum_1b1(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        while (buffer.hasRemaining()) crc32.update(buffer.get());
        return crc32.getValue();
    }

    static long checksum_direct_or_bytes(ByteBuffer buffer) {
        CRC32 crc32 = new CRC32();
        if (buffer.isDirect()) {
            while (buffer.hasRemaining()) crc32.update(buffer.get());
        } else {
            byte[] bytes = buffer.array();
            crc32.update(bytes, buffer.position(), lengthOf(buffer));
        }
        return crc32.getValue();
    }

    static long checksum_bytes(ByteBuffer buffer) {
        byte[] bytes = new byte[lengthOf(buffer)];
        buffer.get(bytes);
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return crc32.getValue();
    }
}
