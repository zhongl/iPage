package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.ReadOnlyMappedBuffers;
import com.github.zhongl.util.Tuple;
import com.google.common.collect.Iterators;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import static java.util.Collections.unmodifiableList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyIndex extends Index {

    protected ReadOnlyIndex(final List<Tuple> tuples) {
        super(unmodifiableList(new PageList(tuples)));

    }

    public double aliveRadio(int delta) {
        int alives = 0;
        for (Entry<Key, Range> entry : entries()) {
            if (!entry.value().equals(Range.NIL)) alives++;
        }
        return (alives + delta) * 1.0 / size();
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

    private int size() {
        int sum = 0;
        for (Page page : pages) sum += ((InnerPage) page).size;
        return sum;
    }

    private static class InnerPage extends Index.InnerPage {

        private final int size;

        protected InnerPage(File file, Key key) {
            super(file, key);
            size = (int) (file.length() / ENTRY_LENGTH);
        }

        @Override
        protected ByteBuffer buffer() {
            return ReadOnlyMappedBuffers.getOrMap(file);
        }

        Collection<Entry<Key, Range>> entries() {
            return new AbstractList<Entry<Key, Range>>() {
                @Override
                public Entry<Key, Range> get(int index) {
                    int position = index * ENTRY_LENGTH;
                    ByteBuffer byteBuffer = (ByteBuffer) buffer().position(position);
                    byte[] bytes = new byte[Key.BYTE_LENGTH];
                    byteBuffer.get(bytes);
                    Key key = new Key(bytes);
                    Range range = new Range(byteBuffer.getLong(), byteBuffer.getLong());
                    return new Entry<Key, Range>(key, range);
                }

                @Override
                public int size() {
                    return size;
                }
            };
        }

    }

    private static class PageList extends AbstractList<Page> implements RandomAccess {
        private final List<Tuple> tuples;

        public PageList(List<Tuple> tuples) {this.tuples = tuples;}

        @Override
        public Page get(int index) {
            Tuple tuple = tuples.get(index);
            return new InnerPage(tuple.<File>get(1), tuple.<Key>get(0));
        }

        @Override
        public int size() {
            return tuples.size();
        }
    }
}
