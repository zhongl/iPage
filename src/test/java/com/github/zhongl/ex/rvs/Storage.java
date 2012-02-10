package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.Nils;
import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.util.SortedSet;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Storage<K, V> implements Iterable<V> {

    public void merge(SortedSet<Entry<K, V>> entries, FutureCallback<Void> flushedCallback) {
        if (entries.isEmpty()) return;
        begin();
        for (Entry<K, V> entry : entries) merge(entry);
        try {
            force();
            flushedCallback.onSuccess(Nils.VOID);
            commit();
        } catch (IOException e) {
            rollback();
            flushedCallback.onFailure(e);
        }
    }

    public abstract V get(K key);

    protected abstract void begin();

    protected abstract void commit();

    protected abstract void rollback();

    protected abstract void force() throws IOException;

    protected abstract void merge(Entry<K, V> entry);
}
