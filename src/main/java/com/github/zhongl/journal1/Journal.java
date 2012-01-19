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
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closeable {
    private final Pages pages;

    public Journal(File dir, Codec... compoundCodecs) {
        Codec[] codecs = Arrays.copyOf(compoundCodecs, compoundCodecs.length + 1);
        codecs[compoundCodecs.length] = Checkpoint.CODEC;
        Codec codec = ComposedCodecBuilder.compose(new CompoundCodec(codecs))
                .with(ChecksumCodec.class)
                .with(LengthCodec.class)
                .build();
        pages = new Pages(dir, codec);
        tryRecover();
    }

    public long append(final Object event, boolean force) {
        return pages.append(event, force);
    }

    public void saveCheckpoint(long number) {
        pages.append(new Checkpoint(number), true); // TODO try append but not force.
        pages.trimBefore(number);
    }

    public void replayTo(Applicable<?> applicable) {
        try {
            for (Cursor cursor = pages.head(); ; cursor = pages.next(cursor)) {
                if (cursor.get() instanceof Checkpoint) continue;
                apply(cursor, applicable);
            }
        } catch (EOFException ignore) {
            pages.reset();
        }
    }

    @Override
    public void close() throws IOException {
        pages.close();
    }

    private void tryRecover() {
        pages.trimBefore(pages.last(Checkpoint.class).number);
    }

    private void apply(final Cursor cursor, Applicable<?> applicable) {
        applicable.apply(new Record() {
            @Override
            public long number() {
                return cursor.position();
            }

            @Override
            public <T> T content() {
                return (T) cursor.get();
            }
        });
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
