package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.page.Appendable;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.ex.page.Page;
import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.Nils;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FixedIndex extends Index {

    static final String PARTITION_NUM = System.getProperty("ipage.fixed.index.partition.num", "128");

    static final BigInteger INTERVAL = Md5Key.MAX.bigInteger.divide(new BigInteger(PARTITION_NUM, 10));

    public FixedIndex(File dir) throws IOException { super(dir); }

    @Override
    protected Snapshot newSnapshot(File file, Codec codec) throws IOException {
        return new InnerSnapshot(file, codec);
    }

    private class InnerSnapshot extends Snapshot {

        public InnerSnapshot(File file, Codec codec) throws IOException {
            super(file, codec);
            int partitionNums = Integer.parseInt(PARTITION_NUM);
            if (pages.size() == 1) {
                Page page = pages.get(0);
                for (int i = 0; i < partitionNums - 1; i++) {
                    page = newPage(page);
                    checkState(page.file().createNewFile());
                    pages.add(page);
                }
            } else {
                checkArgument(pages.size() == partitionNums, "Invalid fixed index snapshot.");
            }
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            if (last == null) return Md5Key.MIN;
            Md5Key number = (Md5Key) parseNumber(last.file().getName());
            BigInteger bigInteger = number.bigInteger.add(INTERVAL);
            return new Md5Key(bigInteger);
        }

        @Override
        public boolean isEmpty() {
            for (Page page : pages)
                if (page.file().length() > 0) return false;
            return true;
        }

        @Override
        protected Snapshot newSnapshotOn(File dir) throws IOException {
            return new InnerSnapshot(dir, codec);
        }

        @Override
        protected void merge(Iterator<Entry<Md5Key, Cursor>> sortedIterator, final Snapshot snapshot) throws IOException {
            PeekingIterator<Entry<Md5Key, Cursor>> bItr = peekingIterator(sortedIterator);
            PeekingIterator<Entry<Md5Key, Cursor>> aItr;

            for (int i = 0; i < pages.size(); i++) {

                Partition partition = (Partition) pages.get(i);
                Md5Key key = i + 1 < pages.size() ? (Md5Key) pages.get(i + 1).number() : Md5Key.MAX;
                /*
                   MD5_MIN                                                                      MD5_MAX
                      |----------|----------|----------|----------|----------|----------|----------|
                      |   p[0]   |   p[1]   |                 p[...]                    |   p[n]   |
                                 ^
                           lower boundary
                   Lower boundary help elements of sortedIterator put into right partition, while it can not be appended
                   because of Cursor.NIL.
                 */
                Entry<Md5Key, Cursor> lowerBoundary = new Entry<Md5Key, Cursor>(key, Nils.CURSOR);
                aItr = peekingIterator(concat(partition.iterator(), singletonIterator(lowerBoundary)));

                final int index = i;
                Appendable<Object> appendable = new Appendable<Object>() {
                    @Override
                    public void append(Object value, FutureCallback<Cursor> callback) {
                        ((InnerSnapshot) snapshot).appendToPartition(index, value, callback);
                    }

                    @Override
                    public void force() {
                        snapshot.force();
                    }
                };

                merge(aItr, bItr, appendable);

                if (aItr.hasNext()) mergeRestOf(aItr, appendable);

                ((InnerSnapshot) snapshot).force(index); // to fix never force bug cause by lower boundary.
            }

        }

        private void force(int index) throws IOException {
            pages.get(index).force();
        }

        private void appendToPartition(int index, Object value, FutureCallback<Cursor> callback) {
            pages.get(index).append(value, callback);
        }

        @Override
        protected Page newPage(File file, Number number, Codec codec) {
            return new Partition(file, number, codec) {

                @Override
                protected boolean isOverflow() {
                    // FIXME a better implement.
                    return false;
                }
            };
        }
    }
}
