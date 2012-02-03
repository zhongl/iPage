package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Appendable;
import com.github.zhongl.ex.page.Number;
import com.google.common.collect.PeekingIterator;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class Snapshot extends Binder {
    public Snapshot(File dir, Codec codec) throws IOException {super(dir, codec);}

    @Override
    protected Page newPage(File file, Number number, Codec codec) {
        return new Partition(file, number, capacity(), codec);
    }

    public Offset get(Md5Key key) {
        try {
            return ((Partition) pages.get(binarySearchPageIndex(key))).get(key);// index will always in [0, pages.size)
        } catch (RuntimeException e) {
            System.out.println(key);
            throw e;
        }
    }

    protected abstract boolean isEmpty();

    public Snapshot merge(Iterator<Entry<Md5Key, Offset>> sortedIterator) throws IOException {

        if (isEmpty()) {
            merge(sortedIterator, this);
            return this;
        }

        File parentFile = dir.getParentFile();
        String child = (Long.parseLong(dir.getName()) + 1) + "";
        Snapshot snapshot = newSnapshotOn(new File(parentFile, child));

        merge(sortedIterator, snapshot);

        remove();

        return snapshot;
    }

    protected abstract <T extends Snapshot> T newSnapshotOn(File dir) throws IOException;

    public void remove() {
        while (!pages.isEmpty()) removeHeadPage();
        checkState(dir.delete());
    }

    protected abstract void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator, Snapshot snapshot) throws IOException;

    @Override
    protected Number parseNumber(String text) { return new Md5Key(text); }

    protected abstract int capacity();

    protected final void merge(PeekingIterator<Entry<Md5Key, Offset>> aItr, PeekingIterator<Entry<Md5Key, Offset>> bItr, Appendable appendable) throws IOException {

        while (aItr.hasNext() && bItr.hasNext()) {

            Entry<Md5Key, Offset> a = aItr.peek();
            Entry<Md5Key, Offset> b = bItr.peek();
            Entry<Md5Key, Offset> c;

            int result = a.compareTo(b);

            if (result < 0) { // a < b
                c = a;
                a = aItr.next();
            } else if (result > 0) { // a > b
                c = b;
                b = bItr.next();
            } else { // a == b, use b instead a
                c = b;
                boolean removed = b.value() == Offset.NIL;
                a = aItr.next();
                b = bItr.next();
                if (removed) continue; // remove entry
            }

            boolean force = !aItr.hasNext() && !bItr.hasNext();
            appendable.append(c, force);
        }

    }

    protected final void mergeRestOf(Iterator<Entry<Md5Key, Offset>> iterator, Appendable merged) throws IOException {
        while (iterator.hasNext()) merged.append(iterator.next(), !iterator.hasNext());
    }
}
