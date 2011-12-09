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
import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static com.github.zhongl.util.ByteBuffers.slice;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link Bucket} has 163
 * {@link com.github.zhongl.index.Slot} for storing tuple of
 * {@link com.github.zhongl.index.Md5Key} and offset of {@link com.github.zhongl.kvengine.Entry} in
 * {@link com.github.zhongl.ipage.IPage}.
 * <p/>
 * Every slot has a head byte to indicate it is empty, occupied or released.
 */
class Bucket implements ValidateOrRecover<Slot, IOException> {

    public static final int LENGTH = Integer.getInteger("com.github.zhongl.ipage.bucket.length", 4096); // default 4K
    private final ByteBuffer buffer;

    public Bucket(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Long put(Md5Key key, Long offset) {
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

    public Long get(Md5Key key) {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.EMPTY) return FileHashTable.NULL_OFFSET; // because rest slots are all empty
            if (slot.state() == Slot.State.OCCUPIED && slot.key().equals(key)) return slot.offset();
        }

        return FileHashTable.NULL_OFFSET;
    }

    public Long remove(Md5Key key) {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.EMPTY) return FileHashTable.NULL_OFFSET; // because rest slots are all empty
            if (slot.state() == Slot.State.OCCUPIED && slot.key().equals(key)) {
                return slot.release();
            }
        }
        return FileHashTable.NULL_OFFSET;
    }

    public void updateCRC() {
        CommonAccessors.LONG.write(calculateCRC(), slice(buffer, LENGTH - 8));
    }

    public boolean checkCRC() {
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

    public int occupiedSlots() {
        int occupiedSlots = 0;
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.OCCUPIED) occupiedSlots++;
        }
        return occupiedSlots;
    }

    private int amountOfSlots() {return LENGTH / Slot.LENGTH;}

    private Slot slots(int index) {
        checkArgument(index >= 0 && index < amountOfSlots());
        return new Slot(slice(buffer, index * Slot.LENGTH, Slot.LENGTH));
    }

    private long calculateCRC() {
        byte[] allSlotBytes = new byte[Slot.LENGTH * amountOfSlots()];
        buffer.position(0);
        buffer.get(allSlotBytes);
        CRC32 crc32 = new CRC32();
        crc32.update(allSlotBytes);
        return crc32.getValue();
    }

    private long readCRC() {
        return CommonAccessors.LONG.read(slice(buffer, LENGTH - 8));
    }
}
