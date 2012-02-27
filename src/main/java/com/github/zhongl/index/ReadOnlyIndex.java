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

package com.github.zhongl.index;

import com.github.zhongl.page.Page;
import com.github.zhongl.util.Entry;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static java.util.Collections.unmodifiableList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyIndex extends Index {

    public ReadOnlyIndex(final List<Entry<File, Key>> entries) {
        super(toPages(entries));
    }

    private static List<Page> toPages(final List<Entry<File, Key>> entries) {
        return unmodifiableList(new ArrayList<Page>(new AbstractList<Page>() {

            @Override
            public Page get(int index) {
                Entry<File, Key> entry = entries.get(index);
                return new InnerPage(entry.key(), entry.value());
            }

            @Override
            public int size() {
                return entries.size();
            }
        }));
    }

    public Collection<Entry<Key, Range>> entries() {
        return new AbstractCollection<Entry<Key, Range>>() {
            @Override
            public Iterator<Entry<Key, Range>> iterator() {
                Iterator[] itrs = new Iterator[pages.size()];
                for (int i = 0; i < itrs.length; i++) {
                    itrs[i] = ((InnerPage) pages.get(i)).entries().iterator();
                }
                return Iterators.concat(itrs);
            }

            @Override
            public int size() {
                return ReadOnlyIndex.this.size();
            }
        };
    }

    public int size() {
        int sum = 0;
        for (Page page : pages) sum += ((InnerPage) page).size;
        return sum;
    }

    public void foreachEntry(Function<Boolean, Void> function) {
        for (Entry<Key, Range> entry : entries())
            function.apply(!entry.value().equals(Range.NIL));
    }

    private static class InnerPage extends Index.InnerPage {
        private final int size;
        private final MappedByteBuffer buffer; // reclaimed by GC.

        protected InnerPage(File file, Key key) {
            super(file, key);
            size = (int) (file.length() / ENTRY_LENGTH);
            try {
                buffer = Files.map(file, FileChannel.MapMode.PRIVATE);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        Collection<Entry<Key, Range>> entries() {
            return new AbstractList<Entry<Key, Range>>() {
                @Override
                public Entry<Key, Range> get(final int index) {
                    int position = index * ENTRY_LENGTH;
                    ByteBuffer duplicate = buffer();
                    Key key = getKey(duplicate, position);
                    Range range = new Range(duplicate.getLong(), duplicate.getLong());
                    return new Entry<Key, Range>(key, range);
                }

                @Override
                public int size() { return size; }
            };
        }

        @Override
        protected ByteBuffer buffer() { return buffer.duplicate(); }
    }

}
