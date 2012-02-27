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

package com.github.zhongl.api;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.codec.LineEntryCodec;
import com.github.zhongl.index.IndexMerger;
import com.github.zhongl.index.Key;
import com.github.zhongl.index.Range;
import com.github.zhongl.index.ReadOnlyIndex;
import com.github.zhongl.line.LineAppender;
import com.github.zhongl.line.Migrater;
import com.github.zhongl.line.ReadOnlyLine;
import com.github.zhongl.util.Entry;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Snapshot<T> implements Iterable<T> {

    private final LineEntryCodec<T> lineEntryCodec;
    private final ReadOnlyLine<T> readOnlyLine;
    private final ReadOnlyIndex readOnlyIndex;
    private final TextFile textFile;

    public Snapshot(@Nullable final File parent, String name, Codec<T> codec) throws IOException {
        textFile = new TextFile(parent, name);
        lineEntryCodec = new LineEntryCodec<T>(codec);
        readOnlyLine = new ReadOnlyLine<T>(textFile.lineEntries(), lineEntryCodec);
        readOnlyIndex = new ReadOnlyIndex(textFile.indexEntres());
    }

    public T get(Key key) {
        return readOnlyLine.get(readOnlyIndex.get(key));
    }

    @Override
    public Iterator<T> iterator() {
        final Iterator<Entry<Key, T>> iterator = readOnlyLine.iterator();
        return new AbstractIterator<T>() {
            @Override
            protected T computeNext() {
                while (iterator.hasNext()) {
                    Entry<Key, T> entry = iterator.next();
                    Range range = readOnlyIndex.get(entry.key());
                    if (!range.equals(Range.NIL)) return entry.value();
                }
                return endOfData();
            }
        };
    }

    protected String append(Collection<Entry<Key, T>> appendings, Collection<Key> removings) {
        SortedSet<Entry<Key, Range>> entries = new TreeSet<Entry<Key, Range>>();

        LineAppender lineAppender = new LineAppender(textFile.parent(), readOnlyLine.length());

        long position = readOnlyLine.length();

        for (Entry<Key, T> entry : appendings) {
            int length = lineAppender.append(lineEntryCodec.encode(entry));
            entries.add(new Entry<Key, Range>(entry.key(), new Range(position, position + length)));
            position += length;
        }

        lineAppender.force();

        for (Key key : removings) entries.add(new Entry<Key, Range>(key, Range.NIL));

        int capacity = readOnlyIndex.entries().size() + appendings.size();

        IndexMerger indexMerger = new IndexMerger(textFile.parent(), capacity) {

            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return false;
            }

        };

        indexMerger.merge(readOnlyIndex.entries().iterator(), entries.iterator());
        indexMerger.force();

        return textFile.create(indexMerger, lineAppender, true).getName();
    }

    protected String defrag(Collection<Entry<Key, T>> appendings, Collection<Key> removings, int capacity) {
        SortedSet<Entry<Key, Range>> entries = new TreeSet<Entry<Key, Range>>();

        for (Entry<Key, T> entry : appendings) entries.add(new Entry<Key, Range>(entry.key(), Range.PLACE_HOLD));
        for (Key key : removings) entries.add(new Entry<Key, Range>(key, Range.NIL));

        final IndexMerger indexMerger = new IndexMerger(textFile.parent(), capacity) {
            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return c.value().equals(Range.NIL);
            }

        };

        indexMerger.merge(readOnlyIndex.entries().iterator(), entries.iterator());

        final LineAppender lineAppender = new LineAppender(textFile.parent(), 0L);

        Migrater migrater = new Migrater() {
            long position;

            @Override
            public void migrate(Key key, ByteBuffer buffer) {
                if (indexMerger.get(key).equals(Range.NIL)) return;
                int length = lineAppender.append(buffer);
                indexMerger.set(key, new Range(position, position + length));
                position += length;
            }
        };

        readOnlyLine.migrateBy(migrater);

        for (Entry<Key, T> entry : appendings) {
            migrater.migrate(entry.key(), lineEntryCodec.encode(entry));
        }

        indexMerger.force();
        lineAppender.force();

        return textFile.create(indexMerger, lineAppender, false).getName();
    }


    public boolean isLinkTo(File file) {
        return textFile.contains(file);
    }

    public int size() {
        return readOnlyIndex.size();
    }

    public void foreachIndexEntry(Function<Boolean, Void> function) {
        readOnlyIndex.foreachEntry(function);
    }
}
