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
public abstract class ReadOnlyLine extends Binder implements Iterable {

    protected ReadOnlyLine(final List<Tuple> tuples) {
        super(unmodifiableList(new PageList(tuples)));
    }

    public <T> T get(Range range) {
        if (pages.isEmpty()) return null;
        return decode(((InnerPage) binarySearch(number(range))).bufferIn(range));
    }

    public void migrateBy(Migrater migrater) {
        for (Page page : pages) {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(page.file());
            byte[] bytes = new byte[Key.BYTE_LENGTH];
            while (buffer.hasRemaining()) {
                buffer.get(bytes);
                int length = buffer.getInt();
                int position = buffer.position();
                Key key = new Key(bytes);
                ByteBuffer slice = (ByteBuffer) buffer.duplicate()
                                                      .limit(position + length)
                                                      .position(position - Key.BYTE_LENGTH - 4);
                migrater.migrate(key, slice);
                buffer.position(position + length);
            }
        }
    }

    public long length() {
        long length = 0;
        for (Page page : pages) length += page.file().length();
        return length;
    }

    @Override
    public Iterator iterator() {
        return new AbstractIterator() {
            Iterator<Page> pItr = pages.iterator();
            ByteBuffer buffer;

            @Override
            protected Object computeNext() {
                if (buffer == null || !buffer.hasRemaining()) {
                    if (!pItr.hasNext()) return endOfData();
                    File file = pItr.next().file();
                    buffer = ReadOnlyMappedBuffers.getOrMap(file);
                }

                byte[] bytes = new byte[Key.BYTE_LENGTH];
                buffer.get(bytes);
                int length = buffer.getInt();
                int position = buffer.position();
                Key key = new Key(bytes);
                ByteBuffer slice = (ByteBuffer) buffer.duplicate()
                                                      .limit(position + length);

                buffer.position(position + length);
                return new Entry<Key, Object>(key, decode(slice));
            }
        };
    }

    protected Number number(Range range) {
        return new Offset(range.from());
    }

    protected abstract <T> T decode(ByteBuffer buffer);

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
