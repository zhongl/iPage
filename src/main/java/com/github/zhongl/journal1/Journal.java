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


import com.github.zhongl.codec.*;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closeable {
    public static final int PAGE_CAPACITY = Integer.getInteger("ipage.journal.page.capacity", 1 << 20 * 64); // 64M
    private final Pages pages;

    public Journal(File dir, Codec... compoundCodecs) {
        Codec[] codecs = Arrays.copyOf(compoundCodecs, compoundCodecs.length + 1);
        codecs[compoundCodecs.length] = Checkpoint.CODEC;
        Codec codec = ComposedCodecBuilder.compose(new CompoundCodec(codecs))
                .with(ChecksumCodec.class)
                .with(LengthCodec.class)
                .build();
        pages = new Pages(dir, codec, PAGE_CAPACITY);
        tryRecover();
    }

    /**
     * Append event.
     *
     * @param event
     * @param force event to driver for duration if it's true.
     *
     * @return checkpoint offset
     */
    public long append(final Object event, boolean force) {
        Record record = pages.append(event, force);
        return record.offset() + record.length();
    }

    /**
     * Save checkpoint and remove applied events before the offset.
     * <p/>
     * The checkpoint can be used for recovery if removing failed because of crashing.
     *
     * @param number
     */
    public void saveCheckpoint(long number) {
        pages.append(new Checkpoint(number), true); // TODO try append but not force.
        pages.trimBefore(number);
    }

    /**
     * Replay unapplied events which after last checkpoint offset to {@link com.github.zhongl.journal1.Applicable}.
     *
     * @param applicable
     */
    public void replayTo(Applicable applicable) {
        for (Record record : pages) {
            if (record.content() instanceof Checkpoint) continue;
            applicable.apply(record);
        }

        applicable.force();
        pages.reset();
    }

    @Override
    public void close() throws IOException {
        pages.close();
    }

    private void tryRecover() {
        long lastCheckpoint = 0L;
        long lastValidPosition = 0L;

        try {
            for (Record record : pages) {
                Object object = record.content();
                if (object instanceof Checkpoint) lastCheckpoint = ((Checkpoint) object).number;
                lastValidPosition = record.offset();
            }
        } catch (IllegalStateException e) { // invalid checksum
            pages.trimAfter(lastValidPosition);
        }

        pages.trimBefore(lastCheckpoint);
    }

    private final static class Checkpoint {

        public static final Codec CODEC = new Codec() {
            @Override
            public ByteBuffer encode(Object instance) {
                Checkpoint checkpoint = (Checkpoint) instance;
                ByteBuffer buffer = ByteBuffer.allocate(8);
                buffer.putLong(0, checkpoint.number);
                return buffer;
            }

            @Override
            public Checkpoint decode(ByteBuffer buffer) {
                return new Checkpoint(buffer.getLong(0));
            }

            @Override
            public boolean supports(Class<?> type) {
                return Checkpoint.class.equals(type);
            }
        };

        private final long number;

        public Checkpoint(long number) { this.number = number; }

    }
}
