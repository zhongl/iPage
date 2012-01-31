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
import com.github.zhongl.ex.page.*;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closable {

    static final int CAPACITY = (1 << 20) * 64;

    private final Binder binder;

    private long revision;

    public Journal(File dir, Codec... codecs) throws IOException {
        final Codec codec = ComposedCodecBuilder.compose(new CompoundCodec(codecs))
                                                .with(ChecksumCodec.class)
                                                .with(LengthCodec.class)
                                                .build();
        binder = new Binder(dir) {

            @Override
            protected Page newPage(File file, long number) {
                return new Page(file, number, CAPACITY, codec) {
                    @Override
                    protected Batch newBatch(CursorFactory cursorFactory, int position, int estimateBufferSize) {
                        return new ParallelEncodeBatch(cursorFactory, position, estimateBufferSize);
                    }
                };
            }

            @Override
            protected long newPageNumber(@Nullable Page last) {
                return revision;
            }
        };

    }

    public long append(Object event, boolean force) throws IOException {
        binder.append(event, force);
        return revision++;
    }

    /**
     * Erase events before the revision.
     *
     * @param revision of journal.
     */
    public void eraseBy(long revision) {
        binder.removePagesFromHeadTo(revision);
    }

    /**
     * Recover unapplied events to {@link com.github.zhongl.ex.journal.Applicable}.
     *
     * @param applicable {@link com.github.zhongl.ex.journal.Applicable}
     */
    public void recover(final Applicable applicable) {
        long checkpoint = applicable.lastCheckpoint();
        long revision = binder.roundPageNumber(checkpoint);

        for (Cursor<Object> cursor = binder.head(checkpoint);
             cursor != null;
             cursor = binder.next(cursor), revision++) {

            if (revision <= checkpoint) continue;
            applicable.apply(cursor.get());
        }

        applicable.force();
        binder.reset();
    }

    @Override
    public void close() {
        binder.close();
    }

}
