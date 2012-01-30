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
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closable {

    static final int CAPACITY = (1 << 20) * 64;

    private final Codec codec;
    private final File dir;
    private final PageFactory factory;
    private final LinkedList<Page> pages;

    public Journal(File dir, PageFactory factory, Codec... codecs) throws IOException {
        this.dir = dir;
        this.factory = factory;
        codec = ComposedCodecBuilder.compose(new CompoundCodec(codecs))
                                    .with(ChecksumCodec.class)
                                    .with(LengthCodec.class)
                                    .build();

        this.pages = loadOrInitialize();
    }

    private LinkedList<Page> loadOrInitialize() throws IOException {
        LinkedList<Page> list = new FilesLoader<Page>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<Page>() {
                    @Override
                    public Page transform(File file, boolean last) throws IOException {
                        return last ? factory.readWritePage(file, CAPACITY) : factory.readOnlyPage(file);
                    }
                }).loadTo(new LinkedList<Page>());

        if (list.isEmpty()) list.add(factory.readWritePage(new File(dir, "0"), CAPACITY));
        return list;
    }

    public interface PageFactory {
        Page readOnlyPage(File file);

        Page readWritePage(File file, int capacity);
    }

    /**
     * Erase events before the offset.
     *
     * @param offset of journal.
     */
    public void erase(long offset) {
        int index = Collections.binarySearch(pages, offset);
        removeFromHeadTo(index);
    }

    private void removeFromHeadTo(int index) {
        for (int i = 0; i < index; i++) pages.remove().delete();
    }

    /**
     * Recover unapplied events to {@link com.github.zhongl.ex.journal.Applicable}.
     *
     * @param applicable {@link com.github.zhongl.ex.journal.Applicable}
     */
    public void recover(final Applicable applicable) {
        try {
            int index = Collections.binarySearch(pages, applicable.lastCheckpoint());
            for (Page page : pages.subList(index, pages.size())) apply(applicable, page);
        } catch (IllegalStateException ignored) { /* invalidChecksum */ }

        applicable.force();
        removeFromHeadTo(pages.size()); // reset
    }

    private void apply(Applicable applicable, Page page) {
        int offset = 0;
        int length = page.length();

        if (applicable.lastCheckpoint() > page.offset()) {
            offset = (int) (applicable.lastCheckpoint() - page.offset());
            length -= offset;
        }

        apply(applicable, page.slice(offset, length));
    }

    private void apply(Applicable applicable, ByteBuffer buffer) {
        while (buffer.hasRemaining()) applicable.apply(codec.decode(buffer));
    }

    @Override
    public void close() {
        for (Page page : pages) {
            page.close();
        }
    }

    public Group<Object> createGroup() {
        return new InnerGroup();
    }

    /**
     * Commit a group of events.
     *
     * @param group of events.
     * @param force to driver if it is true.
     *
     * @return offset of committed.
     */
    public long commit(Group group, boolean force) {
        int appended = pages.getLast().append(group.toBuffer(), force, new OverflowCallback() {
            @Override
            public int onOverflow(ByteBuffer rest, boolean force) {
                Page last = pages.getLast();
                long offset = last.offset() + last.length();
                File file = new File(dir, offset + "");
                Page page = factory.readWritePage(file, CAPACITY);
                int appended = page.append(rest, force, new OverflowThrowing());
                pages.addLast(page);
                return appended;
            }
        });
        return appended + pages.getLast().offset();
    }

    private class InnerGroup implements Group<Object> {
        @Override
        public Group append(Object element) {
            return null;  // TODO commit
        }

        @Override
        public ByteBuffer toBuffer() {
            return null;  // TODO toBuffer
        }
    }
}
