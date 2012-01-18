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

import com.github.zhongl.util.Checksums;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Event {
    static final int FLAG_CRC32_LENGTH = 1 + 8 + 4;

    private final ByteBuffer body;
    private final byte typeCode;

    public static Event readFrom(ByteBuffer raw, boolean validate) {
        byte flag = raw.get();
        long checksum = raw.getLong();
        int length = raw.getInt();
        raw.limit(raw.position() + length);
        ByteBuffer body = raw.slice();
        if (validate) Checksums.validate(body.duplicate(), checksum);
        return new Event(flag, body);
    }

    Event(byte typeCode, ByteBuffer body) {
        this.typeCode = typeCode;
        this.body = body;
    }

    public byte type() {
        return typeCode;
    }

    public long checksum() {
        return Checksums.checksum(body());
    }

    public int length() {
        return body.limit();
    }

    public ByteBuffer body() {
        return body.duplicate();
    }

    public ByteBuffer toBuffer() {
        ByteBuffer packed = ByteBuffer.wrap(new byte[FLAG_CRC32_LENGTH + length()]);
        packed.put(type());
        packed.putLong(checksum());
        packed.putInt(length());
        packed.put(body());
        packed.flip();
        return packed;
    }

}
