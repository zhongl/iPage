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

package com.github.zhongl.index;

import com.github.zhongl.accessor.CommonAccessors;

import java.nio.ByteBuffer;

import static com.github.zhongl.util.ByteBuffers.slice;


/**
 * {@link Slot} = {@link State} {@link Md5Key} {@link Long}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
public class Slot {

    public static final int LENGTH = 1/*head byte*/ + Md5Key.BYTE_LENGTH + 8 /*offset:Long*/;
    private final ByteBuffer buffer;

    public Slot(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public State state() {
        return State.readFrom(buffer.duplicate());
    }

    public Md5Key key() {
        ByteBuffer slice = slice(buffer, 1); // skip head byte
        return Md5Key.ACCESSOR.read(slice);
    }

    public Long offset() {
        ByteBuffer slice = slice(buffer, 1 + Md5Key.BYTE_LENGTH); // skip head byte and key
        return CommonAccessors.LONG.read(slice);
    }

    Long add(Md5Key key, Long offset) {
        ByteBuffer slice = slice(buffer, 0);
        State.OCCUPIED.writeTo(slice);
        Md5Key.ACCESSOR.write(key, slice);
        CommonAccessors.LONG.write(offset, slice);
        return null;
    }

    Long replace(Md5Key key, Long offset) {
        Long previous = offset();
        add(key, offset);
        return previous;
    }

    Long release() {
        State.RELEASED.writeTo(slice(buffer, 0));
        return offset();
    }

    enum State {
        EMPTY, OCCUPIED, RELEASED;

        public void writeTo(ByteBuffer byteBuffer) {
            byteBuffer.put((byte) ordinal());
        }

        public static State readFrom(ByteBuffer byteBuffer) {
            byte b = byteBuffer.get();
            if (EMPTY.ordinal() == b) return EMPTY;
            if (OCCUPIED.ordinal() == b) return OCCUPIED;
            if (RELEASED.ordinal() == b) return RELEASED;
            throw new IllegalStateException("Unknown slot state: " + b);
        }
    }
}
