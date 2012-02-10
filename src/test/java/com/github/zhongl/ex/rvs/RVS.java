package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.FutureCallbacks;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

import static com.github.zhongl.ex.util.FutureCallbacks.getUnchecked;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RVS {


}


class Latency<K, V> {

    Ephemerons<K, V> ephemerons;

    public void append(K key, V value) {
        CallbackFuture<Void> removeCallback = new CallbackFuture<Void>();
        ephemerons.put(key, value, removeCallback);
//        removeCallback.get()
    }

    public void remove(K key) {
        ephemerons.put(key, null, FutureCallbacks.<Void>ignore());
    }
}


abstract class Store<K, V> implements Iterable<V> {

    public void merge(SortedSet<Ephemerons.Record<K, V>> records) {
        if (records.isEmpty()) return;

        Revision revision = null;
        Set<K> removedKeys = new HashSet<K>();

        for (Ephemerons.Record<Entry<K, V>> record : records) {
            revision = getUnchecked(record.revisionFuture());
            Entry<K, V> entry = record.entry();
            merge(entry);
            if (entry.value() == null) removedKeys.add(entry.key());
        }

        force();

        release(removedKeys);

        saveCheckpoint(revision);
    }

    protected abstract void saveCheckpoint(Revision revision);

    protected abstract void release(Set<K> removedKeys);

    private void force() {
        // TODO force
    }

    private void merge(Entry<K, V> entry) {
        // TODO merge
    }

    public V get(K key) {
        return null;  // TODO get
    }

    @Override
    public Iterator<V> iterator() {
        return null;  // TODO iterator
    }
}