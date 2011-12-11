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

import com.github.zhongl.buffer.CommonAccessors;
import com.github.zhongl.buffer.MappedBufferFile;

import static com.github.zhongl.buffer.CommonAccessors.LONG;


/**
 * {@link Slot} = {@link State} {@link Md5Key} {@link Long}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
public class Slot {

    public static final int LENGTH = 1 + Md5Key.BYTE_LENGTH + LONG.byteLengthOf(0L);

    private final int beginPosition;
    private final MappedBufferFile mappedBufferFile;

    public Slot(int beginPosition, MappedBufferFile mappedBufferFile) {
        this.beginPosition = beginPosition;
        this.mappedBufferFile = mappedBufferFile;
    }

    public State state() {
        return State.readFrom(mappedBufferFile, beginPosition);
    }

    public Md5Key key() {
        return mappedBufferFile.readBy(Md5Key.ACCESSOR, beginPositionOfKey(), Md5Key.BYTE_LENGTH);
    }

    public Long offset() {
        return mappedBufferFile.readBy(LONG, beginPositionOfOffset(), LONG.byteLengthOf(0L));
    }

    Long add(Md5Key key, Long offset) {
        State.OCCUPIED.writeTo(mappedBufferFile, beginPosition);
        mappedBufferFile.writeBy(Md5Key.ACCESSOR, beginPositionOfKey(), key);
        mappedBufferFile.writeBy(LONG, beginPositionOfOffset(), offset);
        return null;
    }

    private int beginPositionOfOffset() {return beginPosition + 1 + Md5Key.BYTE_LENGTH;}

    private int beginPositionOfKey() {return beginPosition + 1;}

    Long replace(Md5Key key, Long offset) {
        Long previous = offset();
        add(key, offset);
        return previous;
    }

    Long release() {
        State.RELEASED.writeTo(mappedBufferFile, beginPosition);
        return offset();
    }

    enum State {
        EMPTY, OCCUPIED, RELEASED;

        public void writeTo(MappedBufferFile mappedBufferFile, int offset) {
            mappedBufferFile.writeBy(CommonAccessors.BYTE, offset, (byte) ordinal());
        }

        public static State readFrom(MappedBufferFile mappedBufferFile, int offset) {
            Byte b = mappedBufferFile.readBy(CommonAccessors.BYTE, offset, 1);
            if (EMPTY.ordinal() == b) return EMPTY;
            if (OCCUPIED.ordinal() == b) return OCCUPIED;
            if (RELEASED.ordinal() == b) return RELEASED;
            throw new IllegalStateException("Unknown slot state: " + b);
        }
    }

}
