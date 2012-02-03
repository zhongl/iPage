package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class FlexIndex extends Index {

    static final int MAX_ENTRY_SIZE = Integer.getInteger("ipage.index.page.max.entry.size", 1024 * 43);

    public FlexIndex(File dir) throws IOException {
        super(dir);
    }

    @Override
    protected Snapshot newSnapshot(File file, EntryCodec codec) throws IOException {return new Snapshot(file, codec);}

    @Override
    protected Snapshot merge(Iterator<Entry<Md5Key, Offset>> aItr, Iterator<Entry<Md5Key, Offset>> bItr) throws IOException {
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
                // TODO improve performance
                /*


                            |    p[0]     |     p[1]    |
                  current : |-------------|-------------|-------------|
                            ^                  ^
                            a                  b

                  In above case, p[0] should transfer to new snapshot instead copy entry one by one.
                 */
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

    @Override
    protected void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator, Snapshot next) throws IOException {
        if (sortedIterator instanceof Snapshot.EntryIterator) {
            ((Snapshot.EntryIterator) sortedIterator).transferTo(next);
            return;
        }
        while (sortedIterator.hasNext()) {
            next.append(sortedIterator.next(), !sortedIterator.hasNext());
        }
    }

}

