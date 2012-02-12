package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.github.zhongl.ex.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.annotation.ManagedOperation;
import org.softee.management.annotation.Parameter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
@MBean
public abstract class Ephemerons<K extends Comparable<K>, V> {

    private final Semaphore flowControl = new Semaphore(0, true);
    private final AtomicLong id = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    private final ConcurrentMap<K, Record> map;

    protected Ephemerons(ConcurrentMap<K, Record> map) {this.map = map;}

    @ManagedOperation
    public int throughout(@Parameter("delta") int delta) {
        if (delta > 0) flowControl.release(delta);
        if (delta < 0) flowControl.acquireUninterruptibly(-delta);
        return flowControl.availablePermits();
    }

    @ManagedAttribute
    public boolean isFlushing() {
        return flushing.get();
    }

    @ManagedOperation
    public int size() {
        return map.size();
    }

    public void add(K key, V value, FutureCallback<Void> removedOrDurableCallback) {
        put(checkNotNull(key), checkNotNull(value), checkNotNull(removedOrDurableCallback));
    }

    public void remove(K key) {
        put(checkNotNull(key), (V) Nils.OBJECT, FutureCallbacks.<Void>ignore());
    }

    public V get(K key) {
        Record record = map.get(key);
        return record == null ? getMiss(key) : record.value;
    }

    public void flush() {
        if (!flushing.compareAndSet(false, true)) return;

        final Collection<Entry<K, V>> appendings = new TreeSet<Entry<K, V>>();
        final Collection<K> removings = new ArrayList<K>();

        for (Record record : map.values()) {
            if (record.value == Nils.OBJECT) removings.add(record.key);
            else appendings.add(new Entry<K, V>(record.key, record.value));
        }

        requestFlush(appendings, removings, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void v) {
                foreachKey(new Function<K, Void>() {
                    @Override
                    public Void apply(@Nullable K key) {
                        release(key, Nils.VOID);
                        return Nils.VOID;
                    }

                });
            }

            @Override
            public void onFailure(final Throwable t) {
                foreachKey(new Function<K, Void>() {
                    @Override
                    public Void apply(@Nullable K key) {
                        release(key, t);
                        return Nils.VOID;
                    }
                });
            }

            private void foreachKey(Function<K, Void> function) {
                for (K key : removings) function.apply(key);
                for (Entry<K, V> entry : appendings) function.apply(entry.key());
                flushing.set(false);
            }
        });
    }

    /** This method supposed be asynchronized. */
    protected abstract void requestFlush(Collection<Entry<K, V>> appendings, Collection<K> removings, FutureCallback<Void> flushedCallback);

    /** This method supposed be thread safed. */
    protected abstract V getMiss(K key);

    private void put(K key, V value, FutureCallback<Void> removedOrDurableCallback) {
        release(key, Nils.VOID);
        try {
            while (!flowControl.tryAcquire(500L, TimeUnit.MILLISECONDS)) flush();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        map.put(key, new Record(id.getAndIncrement(), key, value, removedOrDurableCallback));
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
