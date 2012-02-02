package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;
import com.google.common.collect.AbstractIterator;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Index implements Closable {

    static final int MAX_ENTRY_SIZE = Integer.getInteger("ipage.index.page.max.entry.size", 1024 * 10);

    private static final int CAPACITY = MAX_ENTRY_SIZE * EntryCodec.LENGTH;

    private InnerBinder currentBinder;

    public Index(File dir) throws IOException {
        final EntryCodec codec = new EntryCodec();
        ArrayList<InnerBinder> list = new FilesLoader<InnerBinder>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<InnerBinder>() {
                    @Override
                    public InnerBinder transform(File file, boolean last) throws IOException {
                        return new InnerBinder(file, codec);
                    }
                }
        ).loadTo(new ArrayList<InnerBinder>());

        if (list.isEmpty()) {
            currentBinder = new InnerBinder(new File(dir, "0"), codec);
        } else {
            // In this case, it means index merged failed last time because of crash.
            // So the simplest way is keep only the first binder, and remove rest, then wait for recovery.
            currentBinder = list.get(0);
            for (InnerBinder innerBinder : list) innerBinder.remove();
        }
    }

    public void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator) throws IOException {
        if (!sortedIterator.hasNext()) return;

        Iterator<Entry<Md5Key, Offset>> currentIterator = currentBinder.iterator();
        if (!currentIterator.hasNext()) { // current is empty
            merge(sortedIterator, currentBinder);
            return;
        }

        InnerBinder next = merge(currentIterator, sortedIterator);

        merge(currentIterator, next); // merge rest if it has
        merge(sortedIterator, next);  // merge rest if it has

        currentBinder.remove();
        currentBinder = next;
    }

    private InnerBinder merge(Iterator<Entry<Md5Key, Offset>> aItr, Iterator<Entry<Md5Key, Offset>> bItr) throws IOException {
        InnerBinder next = currentBinder.newNextBinder();
        Entry<Md5Key, Offset> a = null;
        Entry<Md5Key, Offset> b = null;

        while ((a != null || aItr.hasNext()) // is aItr run out ?
                && (b != null || bItr.hasNext())) { // is bItr run out?

            if (a == null) a = aItr.next();
            if (b == null) b = bItr.next();

            Entry<Md5Key, Offset> c;
            int result = a.compareTo(b);

            if (result < 0) { // a < b
                c = a;
                a = null;
            } else if (result > 0) { // a > b
                c = b;
                b = null;
            } else { // a == b, use b instead a
                c = b;
                boolean removed = b.value() == Offset.NIL;
                a = null;
                b = null;
                if (removed) continue; // remove entry
            }

            next.append(c, (!aItr.hasNext() && !bItr.hasNext()));
        }

        boolean force = !aItr.hasNext() && !bItr.hasNext();
        if (a != null) next.append(a, force); // a != null && aItr.hasNext() == false
        if (b != null) next.append(b, force); // b != null && bItr.hasNext() == false
        return next;
    }

    private void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator, InnerBinder next) throws IOException {
        while (sortedIterator.hasNext()) {
            next.append(sortedIterator.next(), !sortedIterator.hasNext());
        }
    }

    public Offset get(Md5Key key) {
        return currentBinder.get(key);
    }

    @Override
    public void close() {
        currentBinder.close();
    }

    private class InnerBinder extends Binder implements Iterable<Entry<Md5Key, Offset>> {
        private Entry<Md5Key, Offset> currentAppendingEntry;

        protected InnerBinder(File dir, Codec codec) throws IOException {
            super(dir, codec);
        }

        @Override
        public <T> Cursor append(T value, boolean force) throws IOException {
            currentAppendingEntry = (Entry<Md5Key, Offset>) value;
            return super.append(value, force);
        }

        @Override
        protected Page newPage(File file, Number number, Codec codec) {
            return new InnerPage(file, number, CAPACITY, codec);
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            return currentAppendingEntry == null ? Md5Key.MIN : currentAppendingEntry.key();
        }

        @Override
        protected Number parseNumber(String text) {
            return new Md5Key(text);
        }

        public Offset get(Md5Key key) {
            // index will always in [0, pages.size)
            return ((InnerPage) pages.get(binarySearchPageIndex(key))).get(key);
        }

        public void remove() {
            while (!pages.isEmpty()) removeHeadPage();
            checkState(dir.delete());
        }

        public InnerBinder newNextBinder() throws IOException {
            File parentFile = dir.getParentFile();
            String file = (Long.parseLong(dir.getName()) + 1) + "";
            return new InnerBinder(new File(parentFile, file), codec);
        }

        @Override
        public Iterator<Entry<Md5Key, Offset>> iterator() {
            return new AbstractIterator<Entry<Md5Key, Offset>>() {
                int index = 0;
                int offset = 0;

                @Override
                protected Entry<Md5Key, Offset> computeNext() {
                    if (index == pages.size()) return endOfData();

                    File file = pages.get(index).file();
                    if (file.length() == 0) return endOfData();

                    ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file);
                    buffer.position(offset);
                    Entry<Md5Key, Offset> entry = codec.decode(buffer);

                    offset += EntryCodec.LENGTH;
                    if (offset == file.length()) {
                        index += 1;
                        offset = 0;
                    }
                    return entry;
                }
            };
        }
    }

    private class InnerPage extends Page {
        private final Entries entries = new Entries();
        private int count;

        protected InnerPage(File file, Number number, int capacity, Codec codec) {
            super(file, number, capacity, codec);
        }

        @Override
        protected boolean checkOverflow(int size, int capacity) {
            if ((++count) <= MAX_ENTRY_SIZE) return false;
            count = 0;
            return true;
        }

        @Override
        protected Batch newBatch(CursorFactory cursorFactory, int position, int estimateBufferSize) {
            return new DefaultBatch(cursorFactory, position, estimateBufferSize);
        }

        public Offset get(Md5Key key) {
            if (!file().exists()) return null;
            int index = Collections.binarySearch(entries, new Entry<Md5Key, Offset>(key, new Offset(-1L)));
            if (index < 0) return null;
            return entries.get(index).value();
        }

        private class Entries extends AbstractList<Entry<Md5Key, Offset>> implements RandomAccess {

            @Override
            public Entry<Md5Key, Offset> get(int index) {
                ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file());
                buffer.position(index * EntryCodec.LENGTH).limit((index + 1) * EntryCodec.LENGTH);
                return codec().decode(buffer);
            }

            @Override
            public int size() {
                return ReadOnlyMappedBuffers.getOrMap(file()).capacity() / EntryCodec.LENGTH;
            }
        }

    }
}
