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

import com.github.zhongl.util.Entry;
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

    public Snapshot(@Nullable final File file, Codec<T> codec) throws IOException {
        textFile = new TextFile(file);
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
                    if (range != null) return entry.value();
                }
                return endOfData();
            }
        };
    }

    public String merge(Collection<Entry<Key, T>> appendings, Collection<Key> removings, File tmp) {
        if (readOnlyIndex.aliveRadio(-removings.size()) < 0.5)
            return defrag(readOnlyLine, readOnlyIndex, appendings, removings, tmp);
        else
            return append(readOnlyLine, readOnlyIndex, appendings, removings, tmp);
    }

    protected String append(
            ReadOnlyLine readOnlyLine,
            ReadOnlyIndex readOnlyIndex,
            Collection<Entry<Key, T>> appendings,
            Collection<Key> removings, File tmp
    ) {
        SortedSet<Entry<Key, Range>> entries = new TreeSet<Entry<Key, Range>>();

        LineAppender lineAppender = new LineAppender(tmp, readOnlyLine.length());

        long position = readOnlyLine.length();

        for (Entry<Key, T> entry : appendings) {
            int length = lineAppender.append(lineEntryCodec.encode(entry));
            entries.add(new Entry<Key, Range>(entry.key(), new Range(position, position + length)));
            position += length;
        }

        lineAppender.force();

        for (Key key : removings) entries.add(new Entry<Key, Range>(key, Range.NIL));

        int capacity = readOnlyIndex.entries().size() + appendings.size();

        IndexMerger indexMerger = new IndexMerger(tmp, capacity) {

            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return false;
            }

        };

        indexMerger.merge(readOnlyIndex.entries().iterator(), entries.iterator());
        indexMerger.force();

        return textFile.create(indexMerger, lineAppender, tmp, true).getName();
    }

    protected String defrag(
            ReadOnlyLine readOnlyLine,
            final ReadOnlyIndex readOnlyIndex,
            Collection<Entry<Key, T>> appendings,
            Collection<Key> removings, File tmp
    ) {
        SortedSet<Entry<Key, Range>> entries = new TreeSet<Entry<Key, Range>>();

        for (Entry<Key, T> entry : appendings) entries.add(new Entry<Key, Range>(entry.key(), Range.NIL));
        for (Key key : removings) entries.add(new Entry<Key, Range>(key, Range.NIL));

        int capacity = readOnlyIndex.alives() + appendings.size() - removings.size();
        final IndexMerger indexMerger = new IndexMerger(tmp, capacity) {
            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return c.value().equals(Range.NIL);
            }

        };

        indexMerger.merge(readOnlyIndex.entries().iterator(), entries.iterator());

        final LineAppender lineAppender = new LineAppender(tmp, 0L);

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

        return textFile.create(indexMerger, lineAppender, tmp, false).getName();
    }


    public boolean isLinkTo(File file) {
        return textFile.contains(file);
    }

    public int size() {
        return readOnlyIndex.size();
    }

    public int alives() {
        return readOnlyIndex.alives();
    }
}
