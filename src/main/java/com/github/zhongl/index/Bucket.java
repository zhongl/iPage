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

package com.github.zhongl.index;

import com.github.zhongl.sequence.Cursor;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * {@link Bucket}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
class Bucket implements ValidateOrRecover {

    public static final int LENGTH = Integer.getInteger("com.github.zhongl.index.bucket.length", 4096); // default 4K
    private static final int CRC_OFFSET = LENGTH - 8;

    private final int beginPosition;
    private final FileChannel channel;

    Bucket(int beginPosition, FileChannel channel) {
        this.beginPosition = beginPosition;
        this.channel = channel;
    }

    public Cursor put(Md5Key key, Cursor cursor) throws IOException {
        int firstReleased = -1;
        for (int i = 0; i < amountOfSlots(); i++) {
            switch (slots(i).state()) {
                case EMPTY:
                    return slots(i).add(key, cursor);
                case OCCUPIED:
                    if (slots(i).key().equals(key)) return slots(i).replace(key, cursor);
                    break;
                case RELEASED:
                    if (firstReleased < 0) firstReleased = i;
                    break;
            }
            // continue to check rest slots whether contain the key.
        }
        if (firstReleased < 0) throw new BufferOverflowException();
        return slots(firstReleased).add(key, cursor);
    }

    public Cursor get(Md5Key key) throws IOException {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.EMPTY) return Cursor.NULL; // because rest slots are all empty
            if (slot.state() == Slot.State.OCCUPIED && slot.key().equals(key)) return slot.cursor();
        }
        return Cursor.NULL;
    }

    public Cursor remove(Md5Key key) throws IOException {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (slot.state() == Slot.State.EMPTY) return Cursor.NULL; // because rest slots are all empty
            if (slot.state() == Slot.State.OCCUPIED && slot.key().equals(key)) {
                return slot.release();
            }
        }
        return Cursor.NULL;
    }

    public void updateCRC() throws IOException {
        long checksum = calculateCRC();
        channel.position(CRC_OFFSET);
        channel.write(ByteBuffer.wrap(Longs.toByteArray(checksum)));
    }

    public boolean checkCRC() throws IOException {
        if (slots(0).state() == Slot.State.EMPTY) return true;
        return readCRC() == calculateCRC();
    }

    @Override
    public boolean validateOrRecoverBy(Validator validator) throws IOException {
        for (int i = 0; i < amountOfSlots(); i++) {
            Slot slot = slots(i);
            if (validator.validate(slot.key(), slot.cursor())) continue;
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
        return new Slot(index * Slot.LENGTH + beginPosition, channel);
    }

    private long calculateCRC() throws IOException {
        final int length = Slot.LENGTH * amountOfSlots();
        byte[] bytes = new byte[length];
        channel.position(beginPosition);
        channel.read(ByteBuffer.wrap(bytes));
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        return crc32.getValue();
    }

    private long readCRC() throws IOException {
        channel.position(CRC_OFFSET);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        channel.read(buffer);
        return buffer.getLong(0);
    }


}
