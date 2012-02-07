package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.page.Batch;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.ex.page.Page;
import com.github.zhongl.ex.util.Entry;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class FlexIndex extends Index {

    static final int MAX_ENTRY_SIZE = Integer.getInteger("ipage.flex.index.page.max.entry.size", 1024 * 43);

    private static final int CAPACITY = MAX_ENTRY_SIZE * EntryCodec.LENGTH;

    public FlexIndex(File dir) throws IOException { super(dir); }

    @Override
    protected Snapshot newSnapshot(File file, Codec codec) throws IOException {
        return new InnerSnapshot(file, codec);
    }

    private class InnerSnapshot extends Snapshot {

        private Entry<Md5Key, Cursor> currentAppendingEntry;

        InnerSnapshot(File dir, Codec codec) throws IOException { super(dir, codec); }

        @Override
        public boolean isEmpty() {
            return pages.get(0).file().length() == 0;
        }

        @Override
        protected Snapshot newSnapshotOn(File dir) throws IOException {
            return new InnerSnapshot(dir, codec);
        }

        @Override
        protected void merge(Iterator<Entry<Md5Key, Cursor>> sortedIterator, Snapshot snapshot) throws IOException {
            PeekingIterator<Entry<Md5Key, Cursor>> bItr = Iterators.peekingIterator(sortedIterator);
            PeekingIterator<Entry<Md5Key, Cursor>> aItr;

            for (Page page : pages) {
                Partition partition = (Partition) page;
                aItr = Iterators.peekingIterator(partition.iterator());
                merge(aItr, bItr, snapshot);
                if (aItr.hasNext()) mergeRestOf(aItr, snapshot);
            }

            if (bItr.hasNext()) mergeRestOf(bItr, snapshot);
        }

        @Override
        public void append(Object value, FutureCallback<Cursor> callback) {
            currentAppendingEntry = (Entry<Md5Key, Cursor>) value;
            super.append(value, callback);
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            return currentAppendingEntry == null ? Md5Key.MIN : currentAppendingEntry.key();
        }

        @Override
        protected Page newPage(File file, Number number, Codec codec) {
            return new Partition(file, number, codec) {
                private int count;

                @Override
                protected boolean isOverflow() {
                    if ((++count) <= FlexIndex.MAX_ENTRY_SIZE) return false;
                    count = 0;
                    return true;
                }

                @Override
                protected Batch newBatch(int estimateBufferSize) {
                    return super.newBatch(CAPACITY);
                }
            };
        }
    }
}

