package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

import static com.google.common.collect.Collections2.transform;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Storage<K, V> implements Iterable<V> {

    protected volatile Snapshot snapshot;

    protected final File dir;
    protected final File head;

    protected Storage(File dir) {
        this.dir = dir;
        this.head = new File(dir, "HEAD");
        this.snapshot = loadHead();
        cleanUp();
    }

    public void merge(final Collection<Entry<K, V>> appendings, Collection<K> removings, FutureCallback<Void> flushedCallback) {
        if (appendings.isEmpty() && removings.isEmpty()) return;

        File tmp = begin();
        try {

            snapshot.merge(
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
        } catch (Throwable t) {
            rollback(tmp);
            flushedCallback.onFailure(t);
            return;
        }
        flushedCallback.onSuccess(Nils.VOID);
    }

    public V get(K key) {
        return snapshot.get(toKey(key));
    }

    @Override
    public Iterator<V> iterator() {
        return snapshot.iterator();
    }

    protected File begin() {
        File tmp = new File(dir, "tmp");
        tmp.mkdirs();
        return tmp;
    }

    protected void commit(File tmp) throws IOException {
        Snapshot previous = snapshot;
        snapshot = setHead(snapshot(tmp));
        deleteDir(tmp);
        previous.delete();
    }

    protected Snapshot snapshot(File tmp) throws IOException {
        // scan files under tmp, move them to pages and create revision
        return new Snapshot(tmp) {
            @Override
            protected <T> ByteBuffer encode(Entry<Key, T> entry) {
                return null;  // TODO encode
            }

            @Override
            protected <T> T decode(ByteBuffer buffer) {
                return null;  // TODO decode
            }
        };
    }

    protected void rollback(File tmp) {
        deleteDir(tmp);
    }

    protected abstract void cleanUp(); // clean tmp and files which were not linked by revision.

    protected abstract Key toKey(K key);

    protected abstract Snapshot loadHead();

    protected abstract Snapshot setHead(Snapshot snapshot);

    protected abstract void deleteDir(File tmp);

}
