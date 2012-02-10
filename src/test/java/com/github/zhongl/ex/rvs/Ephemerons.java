package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.FutureCallbacks;
import com.github.zhongl.ex.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.annotation.ManagedOperation;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
@MBean
public abstract class Ephemerons<K, V> {

    private static final Object REMOVED = null;

    private final Semaphore flowControl = new Semaphore(0);
    private final AtomicLong id = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    private final ConcurrentMap<K, Record> map;

    protected Ephemerons(ConcurrentMap<K, Record> map) {this.map = map;}

    @ManagedOperation
    public int throughout(int delta) {
        if (delta > 0) flowControl.release(delta);
        if (delta < 0) flowControl.acquireUninterruptibly(-delta);
        return flowControl.availablePermits();
    }

    @ManagedAttribute
    public boolean isFlush() {
        return flushing.get();
    }

    @ManagedAttribute
    public int size() {
        return map.size();
    }

    public void add(K key, V value, FutureCallback<Void> removedOrDurableCallback) {
        put(checkNotNull(key), checkNotNull(value), checkNotNull(removedOrDurableCallback));
    }

    public void remove(K key) {
        put(checkNotNull(key), (V) REMOVED, FutureCallbacks.<Void>ignore());
    }

    public V get(K key) {
        Record record = map.get(key);
        return record == null ? getMiss(key) : record.value;
    }

    public void flush() {
        if (!flushing.compareAndSet(false, true)) return;

        final SortedSet<Entry<K, V>> entries = sort(map.values());

        requestFlush(entries, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void v) {
                for (Entry<K, V> entry : entries) release(entry.key(), Nils.VOID);
                flushing.set(false);
            }

            @Override
            public void onFailure(Throwable t) {
                for (Entry<K, V> entry : entries) release(entry.key(), t);
                flushing.set(false);
            }
        });
    }

    /** This method supposed be thread safed. */
    protected abstract V getMiss(K key);

    /** This method supposed be asynchronized. */
    protected abstract void requestFlush(SortedSet<Entry<K, V>> records, FutureCallback<Void> flushedCallback);

    private void put(K key, V value, FutureCallback<Void> removedOrDurableCallback) {
        release(key, Nils.VOID);
        if (!flowControl.tryAcquire()) flush(); // CAUTION cpu overload
        map.put(key, new Record(id.getAndIncrement(), key, value, removedOrDurableCallback));
    }

    private SortedSet<Entry<K, V>> sort(Collection<Record> records) {
        SortedSet<Entry<K, V>> sorted = new TreeSet<Entry<K, V>>();
        for (Record record : records) sorted.add(new Entry<K, V>(record.key, record.value));
        return sorted;
    }

    private boolean release(K key, Object voidOrThrowable) {
        Record record = map.remove(key);

        if (record == null) return false;

        flowControl.release();
        if (voidOrThrowable == Nils.VOID)
            record.callback.onSuccess(Nils.VOID);
        else
            record.callback.onFailure((Throwable) voidOrThrowable);
        return true;
    }

    protected class Record implements Comparable<Record> {

        private final Long id;
        private final K key;
        private final V value;
        private final FutureCallback<Void> callback;

        public Record(long id, K key, V value, FutureCallback<Void> callback) {
            this.id = id;
            this.key = key;
            this.value = value;
            this.callback = callback;
        }

        @Override
        public int compareTo(Record o) {
            return id.compareTo(o.id);
        }
    }
}
