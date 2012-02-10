package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.CallbackFuture;
import com.google.common.util.concurrent.FutureCallback;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static com.github.zhongl.ex.util.FutureCallbacks.getUnchecked;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RVS {


}


interface Entry<K, V> {
    K key();

    V value();
}

class Recorder<K, V> {
    Journal journal;
    Cache cache;

    void append(Entry<K, V> entry) {
        CallbackFuture<Revision> future = new CallbackFuture<Revision>();
        Record<Entry<K, V>> record = new Record<Entry<K, V>>(future, entry);
        cache.put(record);
        journal.append(entry, future);
//        future.get();
    }


    public void eraseTo(Revision revision) {
        // TODO eraseTo
    }
}

class Record<E> implements Comparable<Record<E>> {
    private final Future<Revision> revisionFuture;
    private final E entry;

    public Record(Future<Revision> revisionFuture, E entry) {
        this.revisionFuture = revisionFuture;
        this.entry = entry;
    }

    public E entry() {
        return entry;
    }

    public Future<Revision> revisionFuture() {
        return revisionFuture;
    }

    @Override
    public int compareTo(Record<E> o) {
        return getUnchecked(revisionFuture).compareTo(getUnchecked(o.revisionFuture));
    }
}

class Journal<K, V> {

    public void append(Entry<K, V> entry, FutureCallback<Revision> callback) {
        // TODO append
    }
}

interface Revision extends Comparable<Revision> {}

//@MBean
class Cache<K, V> {
    Semaphore flowController = new Semaphore(0);

    Map<K, Record<Entry<K, V>>> map;

    Store<K, V> store;

    private void flush() {
        SortedSet<Record<Entry<K, V>>> records = new TreeSet<Record<Entry<K, V>>>();
        for (Record<Entry<K, V>> record : map.values()) {
            records.add(record);
        }
        store.merge(records);
    }

    private boolean needFlush() {
        return false;  // TODO needFlush
    }


    V get(K key) {
        Record<Entry<K, V>> record = map.get(key);
        if (record == null) return getMiss(key);
        Entry<K, V> entry = record.entry();
        if (entry == null) return null;
        return entry.value();
    }

    private V getMiss(K key) {
        return store.get(key);
    }

    public void put(Record<Entry<K, V>> record) {
        flowController.acquireUninterruptibly();
        K key = record.entry().key();
        Record<Entry<K, V>> previous = map.get(key);
        if (previous == null) map.put(key, record);
        else map.put(key, new Record<Entry<K, V>>(previous.revisionFuture(), null));
        if (needFlush()) flush();
    }

    public void remove(Set<K> keys) {
        for (K key : keys) {
            map.remove(key);
            flowController.release();
        }
    }
}


class Store<K, V> implements Iterable<V> {
    Recorder<K, V> recorder;
    Cache<K, V> cache;

    public void merge(SortedSet<Record<Entry<K, V>>> records) {
        if (records.isEmpty()) return;

        Revision revision = null;
        Set<K> removedKeys = new HashSet<K>();

        for (Record<Entry<K, V>> record : records) {
            revision = getUnchecked(record.revisionFuture());
            Entry<K, V> entry = record.entry();
            merge(entry);
            if (entry.value() == null) removedKeys.add(entry.key());
        }

        force(revision);

        cache.remove(removedKeys);

        recorder.eraseTo(revision);
    }

    private void force(Revision revision) {
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