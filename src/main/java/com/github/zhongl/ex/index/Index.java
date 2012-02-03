package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index implements Closable {
    protected Snapshot current;

    protected Index(File dir) throws IOException {
        final EntryCodec codec = new EntryCodec();
        ArrayList<Snapshot> list = new FilesLoader<Snapshot>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<Snapshot>() {
                    @Override
                    public Snapshot transform(File file, boolean last) throws IOException {
                        return newSnapshot(file, codec);
                    }
                }
        ).loadTo(new ArrayList<Snapshot>());

        if (list.isEmpty()) {
            current = newSnapshot(new File(dir, "0"), codec);
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

    protected abstract Snapshot merge(Iterator<Entry<Md5Key, Offset>> aItr, Iterator<Entry<Md5Key, Offset>> bItr) throws IOException;

    protected abstract void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator, Snapshot next) throws IOException;

    public Offset get(Md5Key key) { return current.get(key); }

    @Override
    public void close() { current.close(); }

    protected abstract Snapshot newSnapshot(File file, EntryCodec codec) throws IOException;
}
