package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Index implements Closable {

    static final int MAX_ENTRY_SIZE = Integer.getInteger("ipage.index.page.max.entry.size", 1024 * 10);

    private Snapshot current;

    public Index(File dir) throws IOException {
        final EntryCodec codec = new EntryCodec();
        ArrayList<Snapshot> list = new FilesLoader<Snapshot>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<Snapshot>() {
                    @Override
                    public Snapshot transform(File file, boolean last) throws IOException {
                        return new Snapshot(file, codec);
                    }
                }
        ).loadTo(new ArrayList<Snapshot>());

        if (list.isEmpty()) {
            current = new Snapshot(new File(dir, "0"), codec);
        } else {
            // In this case, it means index merged failed last time because of crash.
            // So the simplest way is keep only the first binder, and remove rest, then wait for recovery.
            current = list.get(0);
            for (Snapshot snapshot : list) snapshot.remove();
        }
    }

    public void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator) throws IOException {
        if (!sortedIterator.hasNext()) return;

        Iterator<Entry<Md5Key, Offset>> currentIterator = current.iterator();
        if (!currentIterator.hasNext()) { // current is empty
            merge(sortedIterator, current);
            return;
        }

        Snapshot next = merge(currentIterator, sortedIterator);

        merge(currentIterator, next); // merge rest if it has
        merge(sortedIterator, next);  // merge rest if it has

        current.remove();
        current = next;
    }

    private Snapshot merge(Iterator<Entry<Md5Key, Offset>> aItr, Iterator<Entry<Md5Key, Offset>> bItr) throws IOException {
        Snapshot next = current.newSnapshot();
        Entry<Md5Key, Offset> a = null;
        Entry<Md5Key, Offset> b = null;

        while ((a != null || aItr.hasNext()) // is aItr run out ?
                && (b != null || bItr.hasNext())) { // is bItr run out?

            if (a == null) a = aItr.next();
            if (b == null) b = bItr.next();

            Entry<Md5Key, Offset> c;
            int result = a.compareTo(b);

            if (result < 0) { // a < b
                c = a;
                a = null;
            } else if (result > 0) { // a > b
                c = b;
                b = null;
            } else { // a == b, use b instead a
                c = b;
                boolean removed = b.value() == Offset.NIL;
                a = null;
                b = null;
                if (removed) continue; // remove entry
            }

            next.append(c, (!aItr.hasNext() && !bItr.hasNext()));
        }

        boolean force = !aItr.hasNext() && !bItr.hasNext();
        if (a != null) next.append(a, force); // a != null && aItr.hasNext() == false
        if (b != null) next.append(b, force); // b != null && bItr.hasNext() == false
        return next;
    }

    private void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator, Snapshot next) throws IOException {
        while (sortedIterator.hasNext()) {
            next.append(sortedIterator.next(), !sortedIterator.hasNext());
        }
    }

    public Offset get(Md5Key key) { return current.get(key); }

    @Override
    public void close() { current.close(); }

}

