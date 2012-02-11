package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Collections2.transform;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Storage<K extends Comparable<K>, V> implements Iterable<V> {

    protected volatile Revision revision;

    protected final File dir;
    protected final File head;

    protected Storage(File dir) {
        this.dir = dir;
        this.head = new File(dir, "HEAD");
        this.revision = loadHead();
        cleanUp();
    }

    public void merge(final Collection<Entry<K, V>> appendings, Collection<K> removings, FutureCallback<Void> flushedCallback) {
        if (appendings.isEmpty() && removings.isEmpty()) return;

        File tmp = begin();
        try {

            revision.merge(
                    transform(appendings, new Function<Entry<K, V>, Entry<Key, V>>() {
                        @Override
                        public Entry<Key, V> apply(@Nullable Entry<K, V> entry) {
                            return new Entry<Key, V>(toKey(entry.key()), entry.value());
                        }
                    }),
                    transform(removings, new Function<K, Key>() {
                        @Override
                        public Key apply(@Nullable K key) {
                            return toKey(key);
                        }
                    }),
                    tmp);

            commit(tmp);
        } catch (IOException e) {
            rollback(tmp);
            flushedCallback.onFailure(e);
            return;
        }
        flushedCallback.onSuccess(Nils.VOID);
    }

    public V get(K key) {
        return revision.get(toKey(key));
    }

    @Override
    public Iterator<V> iterator() {
        return revision.iterator();
    }

    protected File begin() {
        File tmp = new File(dir, "tmp");
        tmp.mkdirs();
        return tmp;
    }

    protected void commit(File tmp) {
        revision = setHead(revision(tmp));
        deleteDir(tmp);
    }

    protected Revision revision(File tmp) {
        // scan files under tmp, move them to pages and create revision
        return new Revision(tmp);
    }

    protected void rollback(File tmp) {
        deleteDir(tmp);
    }

    protected abstract void cleanUp(); // clean tmp and files which were not linked by revision.

    protected abstract Key toKey(K key);

    protected abstract Revision loadHead();

    protected abstract Revision setHead(Revision revision);

    protected abstract void deleteDir(File tmp);

}
