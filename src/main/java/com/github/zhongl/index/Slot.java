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

import com.github.zhongl.nio.CommonAccessors;
import com.github.zhongl.nio.Store;

import java.io.IOException;

import static com.github.zhongl.nio.CommonAccessors.LONG;


/**
 * {@link Slot} = {@link State} {@link Md5Key} {@link Long}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
public class Slot {

    public static final int LENGTH = 1 + Md5Key.BYTE_LENGTH + LONG.byteLengthOf(0L);

    private final int beginPosition;
    private final Store buffer;

    public Slot(int beginPosition, Store buffer) {
        this.beginPosition = beginPosition;
        this.buffer = buffer;
    }

    public State state() throws IOException {
        return State.readFrom(buffer, beginPosition);
    }

    public Md5Key key() throws IOException {
        return buffer.readBy(Md5Key.ACCESSOR, beginPositionOfKey());
    }

    public Long offset() throws IOException {
        return buffer.readBy(LONG, beginPositionOfOffset());
    }

    Long add(Md5Key key, Long offset) throws IOException {
        State.OCCUPIED.writeTo(buffer, beginPosition);
        buffer.writeBy(Md5Key.ACCESSOR, beginPositionOfKey(), key);
        buffer.writeBy(LONG, beginPositionOfOffset(), offset);
        return null;
    }

    private int beginPositionOfOffset() {return beginPosition + 1 + Md5Key.BYTE_LENGTH;}

    private int beginPositionOfKey() {return beginPosition + 1;}

    Long replace(Md5Key key, Long offset) throws IOException {
        Long previous = offset();
        add(key, offset);
        return previous;
    }

    Long release() throws IOException {
        State.RELEASED.writeTo(buffer, beginPosition);
        return offset();
    }

    enum State {
        EMPTY, OCCUPIED, RELEASED;

        public void writeTo(Store buffer, int offset) throws IOException {
            buffer.writeBy(CommonAccessors.BYTE, offset, (byte) ordinal());
        }

        public static State readFrom(Store buffer, int offset) throws IOException {
            Byte b = buffer.readBy(CommonAccessors.BYTE, offset);
            if (EMPTY.ordinal() == b) return EMPTY;
            if (OCCUPIED.ordinal() == b) return OCCUPIED;
            if (RELEASED.ordinal() == b) return RELEASED;
            throw new IllegalStateException("Unknown slot state: " + b);
        }
    }

}
