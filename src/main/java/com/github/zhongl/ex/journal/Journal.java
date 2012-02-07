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
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal extends Binder<Object> {

    private final int pageCapcity;

    public Journal(File dir, int pageCapcity, Codec... codecs) throws IOException {
        super(dir, ComposedCodecBuilder.compose(new CompoundCodec(codecs))
                                       .with(ChecksumCodec.class)
                                       .with(LengthCodec.class)
                                       .build());
        this.pageCapcity = pageCapcity;
    }

    public void erase(Checkpoint checkpoint) {
        int index = binarySearchPageIndex(checkpoint);
        checkState(index > 0, "Invalid checkpoint %s", checkpoint);
        for (int i = 0; i < index; i++) closeAndDelete(pages.remove(0));
    }

    public void recover(final Applicable applicable) throws IOException {
        Checkpoint checkpoint = applicable.lastCheckpoint();
        int index = binarySearchPageIndex(checkpoint);
        checkArgument(index >= 0, "Too small revision %s.", checkpoint);

        for (int i = index; i < pages.size(); i++) {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(pages.get(i).file());

            while (buffer.hasRemaining()) {
                Object record = codec.decode(buffer);
                applicable.apply(record);
            }
        }
        applicable.force();
        reset();
    }

    public Checkpoint checkpoint(Cursor cursor) {
        int index = binarySearchPageIndex(new Checkpoint(((DefaultCursor) cursor).offset()));
        checkState(index >= 0);
        return (Checkpoint) pages.get(index).number();
    }

    @Override
    protected Page newPage(File file, Number number, Codec codec) {
        return new Page(file, number, codec) {

            @Override
            protected boolean isOverflow() {
                return file().length() >= pageCapcity;
            }

            @Override
            protected Batch newBatch(int estimateBufferSize) {
                long position = ((Checkpoint) number()).value() + file().length();
                return new DefaultBatch(codec(), position, estimateBufferSize);
            }
        };
    }

    @Override
    protected Number newNumber(@Nullable Page last) {
        return last == null ? new Checkpoint(0L) : ((Checkpoint) last.number()).add(last.file().length());
    }

    @Override
    protected Number parseNumber(String text) {
        return new Checkpoint(text);
    }

    private void reset() {
        while (!pages.isEmpty()) closeAndDelete(pages.remove(0));
        pages.add(newPage(null));
    }


    private void closeAndDelete(Page page) {
        page.close();
        checkState(page.file().delete());
    }
}
