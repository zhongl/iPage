/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ipage;

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.github.zhongl.util.Entry;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class IndexMerger extends Index {
    public static final int PAGE_CAPACITY = Integer.getInteger("ipage.index.page.capacity", (1 << 20) * 64 / ENTRY_LENGTH);

    private final File dir;
    private final int capacity;

    private int position;
    private InnerPage last;

    protected IndexMerger(File dir, int capacity) {
        super(new ArrayList<Page>());
        this.dir = dir;
        this.capacity = capacity;
    }

    public void merge(Iterator<Entry<Key, Range>> base, Iterator<Entry<Key, Range>> delta) {
        PeekingIterator<Entry<Key, Range>> aItr = Iterators.peekingIterator(base);
        PeekingIterator<Entry<Key, Range>> bItr = Iterators.peekingIterator(delta);

        while (aItr.hasNext() && bItr.hasNext()) {

            Entry<Key, Range> a = aItr.peek();
            Entry<Key, Range> b = bItr.peek();
            Entry<Key, Range> c;

            int result = a.compareTo(b);

            if (result < 0) c = aItr.next();      // a <  b, use a
            else if (result > 0) c = bItr.next(); // a >  b, use b
            else {                                // a == b, use b instead a
                c = b;
                bItr.next();
                aItr.next();
            }

            if (remove(c)) continue; // remove this entry
            append(c);
        }

        mergeRestOf(aItr);
        mergeRestOf(bItr);
    }

    private void mergeRestOf(PeekingIterator<Entry<Key, Range>> aItr) {
        while (aItr.hasNext()) {
            Entry<Key, Range> c = aItr.next();
            if (remove(c)) continue; // remove this entry
            append(c);
        }
    }

    protected abstract boolean remove(Entry<Key, Range> c);

    protected void append(Entry<Key, Range> c) {
        if (last == null || last.isFull()) {
            last = new InnerPage(c.key());
            pages.add(last);
        }
        last.append(c);
        position++;
    }

    public void set(Key key, Range range) {
        ((InnerPage) binarySearch(key)).set(key, range);
    }

    public void force() {
        for (Page page : pages) ((InnerPage) page).force();
    }

    protected class InnerPage extends Index.InnerPage {
        private final MappedByteBuffer buffer;

        protected InnerPage(Key key) {
            super(new File(dir, System.nanoTime() + ".i"), key);

            try {
                int quantity = Math.min(PAGE_CAPACITY, capacity - position);
                buffer = Files.map(file(), FileChannel.MapMode.READ_WRITE, quantity * ENTRY_LENGTH);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        public void force() {
            buffer.force();
            DirectByteBufferCleaner.clean(buffer);
        }

        public void append(Entry<Key, Range> c) {
            buffer.put(c.key().bytes());
            buffer.putLong(c.value().from());
            buffer.putLong(c.value().to());
        }

        public boolean isFull() {
            return !buffer.hasRemaining();
        }

        public void set(Key key, Range range) {
            ByteBuffer duplicate = buffer();
            duplicate.position(positionOf(key));
            duplicate.putLong(range.from()).putLong(range.to());
        }

        @Override
        protected ByteBuffer buffer() {return buffer.duplicate();}

    }
}
