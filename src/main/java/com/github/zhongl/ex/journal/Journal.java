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

package com.github.zhongl.ex.journal;


import com.github.zhongl.ex.codec.*;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closable {

    static final int CAPACITY = Integer.getInteger("ipage.journal.page.capacity", (1 << 20) * 64); // 64MB

    private final InnerBinder binder;

    private Revision revision;

    public Journal(File dir, Codec... codecs) throws IOException {
        revision = new Revision(0L);
        binder = new InnerBinder(dir, ComposedCodecBuilder.compose(new CompoundCodec(codecs))
                                                          .with(ChecksumCodec.class)
                                                          .with(LengthCodec.class)
                                                          .build());
    }


    /**
     * Append an event.
     *
     * @param event         of operation.
     * @param forceCallback for getting revision after force
     */
    public void append(Object event, final FutureCallback<Revision> forceCallback) {
        binder.append(event, new FutureCallback<Cursor>() {
            @Override
            public void onSuccess(Cursor result) {
                forceCallback.onSuccess(revision);
                revision = revision.increment();
            }

            @Override
            public void onFailure(Throwable t) {
                forceCallback.onFailure(t);
            }
        });
    }

    public void force() { binder.force(); }

    /**
     * Erase events before the revision.
     *
     * @param revision of journal.
     */
    public void eraseTo(Revision revision) {
        binder.removePagesFromHeadTo(revision);
    }

    /**
     * Recover unapplied events to {@link com.github.zhongl.ex.journal.Applicable}.
     *
     * @param applicable {@link com.github.zhongl.ex.journal.Applicable}
     *
     * @throws java.io.IOException
     */
    public void recover(final Applicable applicable) throws IOException {
        Revision checkpoint = applicable.lastCheckpoint();
        binder.foreachFrom(checkpoint, applicable);
        binder.reset();
    }

    @Override
    public void close() { binder.close(); }

    private class InnerBinder extends Binder {

        public InnerBinder(File dir, Codec codec) throws IOException {
            super(dir, codec);
        }

        @Override
        protected Page newPage(File file, Number number, Codec codec) {
            return new Page(file, number, codec) {
                @Override
                protected boolean isOverflow() {
                    return file().length() > CAPACITY;
                }

                @Override
                protected Batch newBatch(Kit kit, int position, int estimateBufferSize) {
                    return new ParallelEncodeBatch(kit, position, estimateBufferSize);
                }
            };
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            return revision;
        }

        @Override
        protected Number parseNumber(String text) {
            return new Revision(Long.parseLong(text));
        }

        void reset() {
            while (!pages.isEmpty()) removeHeadPage();
            pages.add(newPage(null));
        }

        void removePagesFromHeadTo(Number number) {
            int i = binarySearchPageIndex(number);
            for (int j = 0; j < i; j++) removeHeadPage();
        }

        public void foreachFrom(Revision checkpoint, Applicable applicable) throws IOException {
            int index = binarySearchPageIndex(checkpoint);
            checkArgument(index >= 0, "Too small revision %s.", checkpoint);

            Revision revision = (Revision) pages.get(index).number();
            for (int i = index; i < pages.size(); i++) {
                ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(pages.get(i).file());

                while (buffer.hasRemaining()) {
                    Object record = codec.decode(buffer);
                    if (revision.compareTo(checkpoint) > 0) {
                        applicable.apply(record);
                    }
                    revision = revision.increment();
                }
            }
            applicable.force();
        }
    }
}
