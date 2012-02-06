package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index implements Closable {
    protected final File dir;
    protected final EntryCodec codec;
    protected volatile Snapshot current;

    protected Index(File dir) throws IOException {
        this.dir = dir;
        codec = new EntryCodec();
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
            current = list.remove(0);
            for (Snapshot snapshot : list) snapshot.remove();
        }
    }

    public void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator) throws IOException {
        if (!sortedIterator.hasNext()) return;
        current = current.merge(sortedIterator);
    }

    @GuardedBy("volatile")
    public Offset get(Md5Key key) { return current.get(key); }

    @Override
    public void close() { current.close(); }

    protected abstract Snapshot newSnapshot(File file, Codec codec) throws IOException;

}
