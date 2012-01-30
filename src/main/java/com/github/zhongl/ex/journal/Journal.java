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
import com.github.zhongl.ex.page.Batch;
import com.github.zhongl.ex.page.Page;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Journal implements Closable {

    static final int CAPACITY = (1 << 20) * 64;

    private final Codec codec;
    private final File dir;
    private final PageFactory factory;
    private final LinkedList<Page> pages;
    private long revision;
    private Batch group;

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
                        return factory.newPage(file, CAPACITY, codec);
                    }
                }).loadTo(new LinkedList<Page>());

        if (list.isEmpty()) list.add(factory.newPage(new File(dir, "0"), CAPACITY, codec));
        return list;
    }

    public long append(Object event, boolean force) throws IOException {
        Record record = new Record(++revision, event);
        group.append(record);
        if (force) {
        }
        return record.revision();
    }

    public interface PageFactory {

        Page newPage(File file, int capacity, Codec codec);
    }

    /**
     * Erase events before the revision.
     *
     * @param revision of journal.
     */
    public void eraseBy(long revision) {
        int size = pages.size();
        for (int i = 0; revisionRangeOf(pages.peek()).compareTo(revision) < 0 && i < size; i++) {
//            pages.remove().delete();
        }
    }

    private RevisionRange revisionRangeOf(Page page) {
        return new RevisionRange(page);
    }

    private void removeFromHeadTo(int index) {
//        for (int i = 0; i < index; i++) pages.remove().delete();
    }

    /**
     * Recover unapplied events to {@link com.github.zhongl.ex.journal.Applicable}.
     *
     * @param applicable {@link com.github.zhongl.ex.journal.Applicable}
     */
    public void recover(final Applicable applicable) {
        eraseBy(applicable.lastCheckpoint());
        try {
            int index = 1/*Collections.binarySearch(pages, applicable.lastCheckpoint())*/;
            for (Page page : pages.subList(index, pages.size())) apply(applicable, page);
        } catch (IllegalStateException ignored) { /* invalidChecksum */ }

        applicable.force();
        removeFromHeadTo(pages.size()); // reset
    }

    private void apply(Applicable applicable, Page page) {
        int offset = 0;
//        int length = page.length();
//
//        if (applicable.lastCheckpoint() > page.offset()) {
//            offset = (int) (applicable.lastCheckpoint() - page.offset());
//            length -= offset;
//        }
//
//        apply(applicable, page.slice(offset, length));
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

    private class RevisionRange implements Comparable<Long> {

        private final long from;
        private final long to;

        public RevisionRange(Page page) {
            File file = page.file();
            from = Long.parseLong(file.getName());
            to = file.length() + from;
        }

        @Override
        public int compareTo(Long revision) {
            if (revision >= to) return -1;
            if (revision < from) return 1;
            return 0;
        }
    }

}
