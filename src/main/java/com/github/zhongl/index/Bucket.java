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

import com.github.zhongl.buffer.Accessor;
import com.github.zhongl.buffer.MappedBufferFile;
import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static com.github.zhongl.buffer.CommonAccessors.LONG;

/**
 * {@link Bucket}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
class Bucket implements ValidateOrRecover<Slot, IOException> {

    public static final int LENGTH = Integer.getInteger("com.github.zhongl.index.bucket.length", 4096); // default 4K
    private static final int CRC_OFFSET = LENGTH - 8;

    private final int beginPosition;
    private final MappedBufferFile mappedBufferFile;

    Bucket(int beginPosition, MappedBufferFile mappedBufferFile) {
        this.beginPosition = beginPosition;
        this.mappedBufferFile = mappedBufferFile;
    }

    public Long put(Md5Key key, Long offset) throws IOException {
        int firstReleased = -1;
        for (int i = 0; i < amountOfSlots(); i++) {
            switch (slots(i).state()) {
                case EMPTY:
                    return slots(i).add(key, offset);
                case OCCUPIED:
                    if (slots(i).key().equals(key)) return slots(i).replace(key, offset);
                    break;
                case RELEASED:
                    if (firstReleased < 0) firstReleased = i;
                    break;
            }
            // continue to check rest slots whether contain the key.
        }
        if (firstReleased < 0) throw new BufferOverflowException();
        return slots(firstReleased).add(key, offset);
    }

    public Long get(Md5Key key) throws IOException {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.EMPTY) return FileHashTable.NULL_OFFSET; // because rest slots are all empty
            if (slot.state() == Slot.State.OCCUPIED && slot.key().equals(key)) return slot.offset();
        }
        return FileHashTable.NULL_OFFSET;
    }

    public Long remove(Md5Key key) throws IOException {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.EMPTY) return FileHashTable.NULL_OFFSET; // because rest slots are all empty
            if (slot.state() == Slot.State.OCCUPIED && slot.key().equals(key)) {
                return slot.release();
            }
        }
        return FileHashTable.NULL_OFFSET;
    }

    public void updateCRC() throws IOException {
        mappedBufferFile.writeBy(LONG, CRC_OFFSET, calculateCRC());
    }

    public boolean checkCRC() throws IOException {
        if (slots(0).state() == Slot.State.EMPTY) return true;
        return readCRC() == calculateCRC();
    }

    @Override
    public boolean validateOrRecoverBy(Validator<Slot, IOException> validator) throws IOException {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (validator.validate(slot)) continue;
            slot.release();
            return false;
        }
        return true;
    }

    public int occupiedSlots() throws IOException {
        int occupiedSlots = 0;
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            Slot.State state = slot.state();
            if (state == Slot.State.EMPTY) break;
            if (state == Slot.State.OCCUPIED) occupiedSlots++;
        }
        return occupiedSlots;
    }

    private int amountOfSlots() {return LENGTH / Slot.LENGTH;}

    private Slot slots(int index) {
        return new Slot(index * Slot.LENGTH + beginPosition, mappedBufferFile);
    }

    private long calculateCRC() throws IOException {
        final int length = Slot.LENGTH * amountOfSlots();
        byte[] allSlotBytes = mappedBufferFile.readBy(new Accessor<byte[]>() {
            @Override
            public int byteLengthOf(byte[] object) { throw new UnsupportedOperationException(); }

            @Override
            public int write(byte[] object, ByteBuffer buffer) { throw new UnsupportedOperationException(); }

            @Override
            public byte[] read(ByteBuffer buffer) {
                byte[] bytes = new byte[length];
                buffer.get(bytes);
                return bytes;
            }
        }, beginPosition, length);

        CRC32 crc32 = new CRC32();
        crc32.update(allSlotBytes);
        return crc32.getValue();
    }

    private long readCRC() throws IOException {
        return mappedBufferFile.readBy(LONG, CRC_OFFSET, LONG.byteLengthOf(0L));
    }


}
