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
import com.github.zhongl.util.Nils;
import com.github.zhongl.util.ReadOnlyMappedBuffers;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class ReadOnlyLine<T> extends Binder implements Iterable<Entry<Key, T>> {

    protected final LineEntryCodec<T> lineEntryCodec;

    protected ReadOnlyLine(final List<Entry<File, Offset>> tuples, LineEntryCodec<T> lineEntryCodec) {
        super(unmodifiableList(new PageList<T>(tuples, lineEntryCodec)));
        this.lineEntryCodec = lineEntryCodec;
    }

    public T get(Range range) {
        if (range.equals(Range.NIL)) return null;
        if (pages.isEmpty()) return null;
        return ((InnerPage<T>) binarySearch(number(range))).get(range);
    }

    public void migrateBy(final Migrater migrater) {
        for (Page page : pages) {
            ReadOnlyMappedBuffers.read(page.file(), new Function<ByteBuffer, Void>() {
                @Override
                public Void apply(@Nullable ByteBuffer buffer) {
                    while (buffer.hasRemaining()) {
                        LineEntryCodec.LazyDecoder<T> decoder = lineEntryCodec.lazyDecoder(buffer);
                        migrater.migrate(decoder.key(), decoder.origin());
                    }
                    return Nils.VOID;
                }
            });
        }
    }

    public long length() {
        long length = 0;
        for (Page page : pages) length += page.file().length();
        return length;
    }

    @Override
    public Iterator<Entry<Key, T>> iterator() {
        return new AbstractIterator<Entry<Key, T>>() {
            Iterator<Page> pItr = pages.iterator();
            Page current;
            int position;

            @Override
            protected Entry<Key, T> computeNext() {
                if (current == null || position >= current.file().length()) {
                    if (!pItr.hasNext()) return endOfData();
                    current = pItr.next();

                    position = 0;
                    if (!current.file().exists()) return endOfData();
                }

                return ReadOnlyMappedBuffers.read(current.file, new Function<ByteBuffer, Entry<Key, T>>() {
                    @Override
                    public Entry<Key, T> apply(@Nullable ByteBuffer buffer) {
                        buffer.position(position);
                        long from = position + ((Offset) current.number()).value();
                        LineEntryCodec.LazyDecoder<T> decoder = lineEntryCodec.lazyDecoder(buffer);
                        position = buffer.position();
                        long to = position + ((Offset) current.number()).value();
                        return new EnhancedEntry<Key, T>(decoder.key(), decoder.value(), new Range(from, to));
                    }
                });
            }
        };
    }

    protected Number number(Range range) {
        return new Offset(range.from());
    }

    public static class EnhancedEntry<K, V> extends Entry<K, V> {

        private final Range range;

        public EnhancedEntry(K key, V value, Range range) {
            super(key, value);
            this.range = range;
        }

        public boolean matchs(Range range) {
            return this.range.equals(range);
        }
    }

    private static class InnerPage<T> extends Page {

        private final LineEntryCodec<T> lineEntryCodec;

        protected InnerPage(File file, Offset number, LineEntryCodec<T> lineEntryCodec) {
            super(file, number);
            this.lineEntryCodec = lineEntryCodec;
        }

        public T get(final Range range) {
            try {
                FileInputStream stream = new FileInputStream(file());
                ByteBuffer buffer = read(stream, refer(range.from()), refer(range.to()));
                return lineEntryCodec.lazyDecoder(buffer).value();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private int refer(long absolute) { return (int) (absolute - ((Offset) number()).value()); }

        private ByteBuffer read(FileInputStream stream, int begin, int end) throws IOException {
            try {
                FileChannel channel = stream.getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(end - begin);
                read(channel, begin, buffer);
                return (ByteBuffer) buffer.flip();
            } finally {
                Closeables.closeQuietly(stream);
            }
        }

        private void read(FileChannel channel, int begin, ByteBuffer buffer) throws IOException {
            try {
                channel.position(begin);
                channel.read(buffer);
            } finally {
                Closeables.closeQuietly(channel);
            }
        }
    }

    private static class PageList<T> extends AbstractList<Page> {
        private final List<Entry<File, Offset>> entries;
        private final LineEntryCodec<T> lineEntryCodec;

        public PageList(List<Entry<File, Offset>> entries, LineEntryCodec<T> lineEntryCodec) {
            this.entries = entries;
            this.lineEntryCodec = lineEntryCodec;
        }

        @Override
        public Page get(int index) {
            Entry<File, Offset> entry = entries.get(index);
            return new InnerPage<T>(entry.key(), entry.value(), lineEntryCodec);
        }

        @Override
        public int size() {
            return entries.size();
        }
    }
}
