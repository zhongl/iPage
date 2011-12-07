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

import com.github.zhongl.ipage.OverflowException;
import com.github.zhongl.kvengine.Md5Key;
import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.io.Files;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link Buckets} is a file-based hash map for mapping
 * {@link com.github.zhongl.kvengine.Md5Key} and offset of {@link com.github.zhongl.kvengine.Record} in
 * {@link com.github.zhongl.ipage.IPage}.
 * <p/>
 * It is a implemente of separate chain hash table, more infomation you can find in "Data Structures & Algorithms In Java".
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
final class Buckets implements Closeable {

    static final Long NULL_OFFSET = null;
    static final int DEFAULT_SIZE = 256;
    private final MappedByteBuffer mappedByteBuffer;
    private final Bucket[] buckets;
    private final File file;
    private int occupiedSlots;

    public Buckets(File file, int size) throws IOException {
        this.file = file;
        size = size > 0 ? size : DEFAULT_SIZE;
        mappedByteBuffer = Files.map(file, READ_WRITE, size * Bucket.LENGTH);
        buckets = createBuckets(size);
    }

    public int size() {
        return buckets.length;
    }

    public Long put(Md5Key key, Long offset) {
        Bucket bucket = buckets[hashAndMod(key)];
        Long preoffset = bucket.put(key, offset);
        bucket.updateDigest();
        if (preoffset == null) occupiedSlots++; // add a new key
        return preoffset;
    }

    public Long get(Md5Key key) {
        return buckets[hashAndMod(key)].get(key);
    }

    public Long remove(Md5Key key) {
        Bucket bucket = buckets[hashAndMod(key)];
        Long preoffset = bucket.remove(key);
        if (preoffset != null) { // remove an exist key
            bucket.updateDigest();
            occupiedSlots--;
        }
        return preoffset;
    }

    @Override
    public void close() throws IOException { // TODO remve Closeable because of IOException
        flush();
        DirectByteBufferCleaner.clean(mappedByteBuffer);
    }

    private int hashAndMod(Md5Key key) {
        return Math.abs(key.hashCode()) % buckets.length;
    }

    private Bucket[] createBuckets(int size) {
        Bucket[] buckets = new Bucket[size];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new Bucket(slice(mappedByteBuffer, i * Bucket.LENGTH, Bucket.LENGTH));
            calculateOccupiedSlotOf(buckets[i]);
        }
        return buckets;
    }

    private void calculateOccupiedSlotOf(Bucket bucket) {
        Bucket.Slot[] slots = bucket.slots;
        for (Bucket.Slot slot : slots) {
            if (slot.state() == Bucket.Slot.State.OCCUPIED) occupiedSlots++;
        }
    }

    public void flush() {
        mappedByteBuffer.force();
    }

    private static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
        buffer.position(offset);
        buffer.limit(offset + length);
        return buffer.slice();
    }

    public int no() {
        return Integer.parseInt(file.getName());
    }

    public void cleanupIfAllKeyRemoved() {
        if (occupiedSlots > 0) return;
        file.delete();
        // TODO log a warn if fail to delete
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Buckets");
        sb.append("{file=").append(file);
        sb.append('}');
        return sb.toString();
    }

//    public boolean validateAndRecoverBy(RecordFinder recordFinder) throws IOException {
//        for (Bucket bucket : buckets) {
//            try {
//                bucket.validate();
//            } catch (UnsafeDataStateException e) {
//                bucket.recoverBy(recordFinder);
//                bucket.updateDigest();
//                return true; // crash can cause only one bucket broken
//            }
//        }
//        return false;
//    }

    /**
     * {@link Buckets.Bucket} has 163
     * {@link Buckets.Bucket.Slot} for storing tuple of
     * {@link Md5Key} and offset of {@link com.github.zhongl.kvengine.Record} in
     * {@link com.github.zhongl.ipage.IPage}.
     * <p/>
     * Every slot has a head byte to indicate it is empty, occupied or released.
     */
    private static class Bucket {

        public static final int LENGTH = Integer.getInteger("com.github.zhongl.ipage.bucket.length", 4096); // default 4K
        private final Slot[] slots;
        private final ByteBuffer buffer;

        public Bucket(ByteBuffer buffer) {
            this.buffer = buffer;
            slots = createSlots(buffer.duplicate());
        }

        private Slot[] createSlots(ByteBuffer buffer) {
            Slot[] slots = new Slot[LENGTH / Slot.LENGTH];
            for (int i = 0; i < slots.length; i++) {
                slots[i] = new Slot(slice(buffer, i * Slot.LENGTH, Slot.LENGTH));
            }
            return slots;
        }

        public Long put(Md5Key key, Long offset) {
            int firstReleased = -1;
            for (int i = 0; i < slots.length; i++) {
                switch (slots[i].state()) {
                    case EMPTY:
                        return slots[i].add(key, offset);
                    case OCCUPIED:
                        if (slots[i].keyEquals(key)) return slots[i].replace(key, offset);
                        break;
                    case RELEASED:
                        if (firstReleased < 0) firstReleased = i;
                        break;
                }
                // continue to check rest slots whether contain the key.
            }
            if (firstReleased < 0) throw new OverflowException();
            return slots[firstReleased].add(key, offset);
        }

        public Long get(Md5Key key) {
            for (Slot slot : slots) {
                if (slot.state() == Slot.State.EMPTY) return NULL_OFFSET; // because rest slots are all empty
                if (slot.state() == Slot.State.OCCUPIED && slot.keyEquals(key)) return slot.offset();
            }
            return NULL_OFFSET;
        }

        public Long remove(Md5Key key) {
            for (Slot slot : slots) {
                if (slot.state() == Slot.State.EMPTY) return NULL_OFFSET; // because rest slots are all empty
                if (slot.state() == Slot.State.OCCUPIED && slot.keyEquals(key)) {
                    return slot.release();
                }
            }
            return NULL_OFFSET;
        }

//        public void validate() throws UnsafeDataStateException {
//            if (slots[0].state() == Slot.State.EMPTY) return;// this is a empty bucket, no need to validate
//            if (!Arrays.equals(readMd5(), calculateMd5())) throw new UnsafeDataStateException();
//        }
//
//        public void recoverBy(RecordFinder recordFinder) throws IOException {
//            for (Slot slot : slots) {
//                Record record = recordFinder.getRecordIn(slot.offset());
//                if (record == null || !slot.keyEquals(Md5Key.valueOf(record))) {
//                    slot.release();
//                    break; // only one slot can be broken if crashed
//                }
//            }
//        }

        public void updateDigest() {
            byte[] md5 = calculateMd5();
            buffer.position(LENGTH - 16);
            buffer.put(md5);
        }

        private byte[] readMd5() {
            byte[] md5 = new byte[16];
            buffer.position(LENGTH - 16);
            buffer.get(md5);
            return md5;
        }

        private byte[] calculateMd5() {
            byte[] allSlotBytes = new byte[Slot.LENGTH * slots.length];
            buffer.position(0);
            buffer.get(allSlotBytes);
            return DigestUtils.md5(allSlotBytes);
        }

        /**
         * {@link Buckets.Bucket.Slot} =
         * {@link Buckets.Bucket.Slot.State}
         * {@link Md5Key}
         * {@link Long}
         */
        private static class Slot {

            public static final int LENGTH = 1/*head byte*/ + Md5Key.LENGTH + 8 /*offset:Long*/;
            private final ByteBuffer buffer;

            public Slot(ByteBuffer buffer) {
                this.buffer = buffer;
            }

            public Long add(Md5Key key, Long offset) {
                buffer.position(0);
                buffer.put(State.OCCUPIED.toByte());
                key.writeTo(buffer);
                buffer.putLong(offset);
                return null;
            }

            public State state() {
                return State.valueOf(buffer.get(0));
            }

            public boolean keyEquals(Md5Key key) {
                buffer.position(1); // skip head byte
                return Md5Key.readFrom(buffer).equals(key);
            }

            public Long replace(Md5Key key, Long offset) {
                Long previous = offset();
                add(key, offset);
                return previous;
            }

            public Long offset() {
                buffer.position(1 + Md5Key.LENGTH); // skip head byte and key
                return buffer.getLong();
            }

            public Long release() {
                buffer.put(0, State.RELEASED.toByte());
                return offset();
            }

            enum State {
                EMPTY, OCCUPIED, RELEASED;

                public byte toByte() {
                    return (byte) this.ordinal();
                }

                public static State valueOf(byte b) {
                    if (EMPTY.toByte() == b) return EMPTY;
                    if (OCCUPIED.toByte() == b) return OCCUPIED;
                    if (RELEASED.toByte() == b) return RELEASED;
                    throw new IllegalStateException("Unknown slot state: " + b);
                }
            }
        }

    }
}
