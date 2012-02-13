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
import com.github.zhongl.util.ReadOnlyMappedBuffers;
import com.github.zhongl.util.Tuple;
import com.google.common.collect.AbstractIterator;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class ReadOnlyLine<T> extends Binder implements Iterable<Entry<Key, T>> {

    protected final LineEntryCodec<T> lineEntryCodec;

    protected ReadOnlyLine(final List<Tuple> tuples, LineEntryCodec<T> lineEntryCodec) {
        super(unmodifiableList(new PageList(tuples)));
        this.lineEntryCodec = lineEntryCodec;
    }

    public <T> T get(Range range) {
        if (range.equals(Range.NIL)) return null;
        if (pages.isEmpty()) return null;
        ByteBuffer buffer = ((InnerPage) binarySearch(number(range))).bufferIn(range);
        LineEntryCodec.LazyDecoder<T> decoder = (LineEntryCodec.LazyDecoder<T>) lineEntryCodec.lazyDecoder(buffer);
        return decoder.value();
    }

    public void migrateBy(Migrater migrater) {
        for (Page page : pages) {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(page.file());
            while (buffer.hasRemaining()) {
                LineEntryCodec.LazyDecoder<T> decoder = lineEntryCodec.lazyDecoder(buffer);
                migrater.migrate(decoder.key(), decoder.origin());
            }
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
            ByteBuffer buffer;

            @Override
            protected Entry<Key, T> computeNext() {
                if (buffer == null || !buffer.hasRemaining()) {
                    if (!pItr.hasNext()) return endOfData();
                    File file = pItr.next().file();
                    buffer = ReadOnlyMappedBuffers.getOrMap(file);
                }
                LineEntryCodec.LazyDecoder<T> decoder = lineEntryCodec.lazyDecoder(buffer);
                return new Entry<Key, T>(decoder.key(), decoder.value());
            }
        };
    }

    protected Number number(Range range) {
        return new Offset(range.from());
    }

    private static class InnerPage extends Page {

        protected InnerPage(File file, Offset number) {
            super(file, number);
        }

        public ByteBuffer bufferIn(Range range) {
            return (ByteBuffer) ReadOnlyMappedBuffers.getOrMap(file())
                                                     .limit(refer(range.to()))
                                                     .position(refer(range.from()));
        }

        private int refer(long absolute) {
            return (int) (absolute - ((Offset) number()).value());
        }
    }

    private static class PageList extends AbstractList<Page> {
        private final List<Tuple> tuples;

        public PageList(List<Tuple> tuples) {this.tuples = tuples;}

        @Override
        public Page get(int index) {
            Tuple tuple = tuples.get(index);
            return new InnerPage(tuple.<File>get(1), tuple.<Offset>get(0));
        }

        @Override
        public int size() {
            return tuples.size();
        }
    }
}
