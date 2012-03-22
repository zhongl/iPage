/*
 * Copyright 2012 zhongl
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.api;

import com.github.zhongl.index.Key;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.FutureCallbacks;
import com.github.zhongl.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.annotation.*;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
@MBean
public abstract class Ephemerons<V> {
    private final AtomicLong id;
    private final Map<Key, Record> map;
    private final Semaphore flowControl;
    private final AtomicBoolean flushing;
    private final ExecutorService asyncRemovingService;

    protected Ephemerons() {
        id = new AtomicLong(0L);
        map = new ConcurrentHashMap<Key, Record>();
        flowControl = new Semaphore(0, true);
        flushing = new AtomicBoolean(false);
        asyncRemovingService = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "async-removing");
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    public void add(final Key key, final V value, final FutureCallback<Void> removedOrDurableCallback) {
        checkNotNull(key);
        checkNotNull(value);
        checkNotNull(removedOrDurableCallback);

        release(key, Nils.VOID);
        acquire();
        map.put(key, new Record(id.getAndIncrement(), key, value, removedOrDurableCallback));
    }

    public void remove(final Key key, final FutureCallback<Void> appliedCallback) {
        checkNotNull(key);
        checkNotNull(appliedCallback);

        if (release(key, Nils.VOID)) {
            appliedCallback.onSuccess(Nils.VOID);
            return;
        }

        acquire();
        map.put(key, new Record(id.getAndIncrement(), key, (V) Nils.OBJECT, appliedCallback));
    }

    public V get(final Key key) {
        checkNotNull(key);
        Record record = map.get(key);
        if (record == null) return getMiss(key);
        if (record.value == Nils.OBJECT) return null;
        return record.value;
    }

    public void flush() {
        if (!flushing.compareAndSet(false, true)) return; // only one can trigger the flushing.

        final Collection<WriteOperation<Entry<Key, V>>> addOrUpdates = new ArrayList<WriteOperation<Entry<Key, V>>>();
        final Collection<WriteOperation<Key>> removes = new ArrayList<WriteOperation<Key>>();

        for (final Record record : new TreeSet<Record>(map.values())) {
            if (record.value.equals(Nils.OBJECT)) {
                removes.add(new WriteOperation<Key>(record.key, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        release(record.key, Nils.VOID);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        release(record.key, t);
                    }

                }));
            } else {
                addOrUpdates.add(new WriteOperation<Entry<Key, V>>(new Entry<Key, V>(record.key, record.value), new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        Record thatRecord = map.get(record.key);

                        if (thatRecord == null) { // Remove the key which had been removed during flushing
                            if (flowControl.tryAcquire()) remove(record.key, FutureCallbacks.<Void>ignore());
                            else {
                                // TODO refactor this ugly implement.
                                asyncRemovingService.submit(new Runnable() {
                                    @Override
                                    public void run() {
                                        remove(record.key, FutureCallbacks.<Void>ignore());
                                    }
                                });
                            }
                            return;
                        }

                        if (thatRecord.compareTo(record) == 0) {
                            map.remove(record.key);
                            flowControl.release();
                            record.callback.onSuccess(result);
                        }

                        // Keep the key which had been updated during flushing
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        //  release if it is the same entry
                        Record thatRecord = map.get(record.key);
                        if (thatRecord != null && thatRecord.compareTo(record) == 0) {
                            map.remove(record.key);
                            flowControl.release();
                            record.callback.onFailure(t);
                        }
                        // do nothing if it is removed or updated
                    }
                }));
            }
        }

        requestFlush(addOrUpdates, removes, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void v) {
                flushing.set(false);
            }

            @Override
            public void onFailure(final Throwable t) {
                throw new UnsupportedOperationException("Should always invoke onSuccess not this method.");
            }

        });
    }

    /** This method supposed be asynchronized. */
    protected abstract void requestFlush(
            Collection<WriteOperation<Entry<Key, V>>> addOrUpdates,
            Collection<WriteOperation<Key>> removes,
            FutureCallback<Void> futureCallback
    );

    /** This method supposed be thread safed. */
    protected abstract V getMiss(Key key);

    @ManagedOperation
    @Description("positive delta for up, negative delta for down.")
    public int throughout(@Parameter("delta") int delta) {
        if (delta > 0) flowControl.release(delta);
        if (delta < 0) flowControl.acquireUninterruptibly(-delta);
        return flowControl.availablePermits();
    }

    @ManagedAttribute
    public boolean isFlushing() { return flushing.get(); }

    @ManagedAttribute
    public int getSize() { return map.size(); }

    private void acquire() {
        try {
            while (!flowControl.tryAcquire(500L, TimeUnit.MILLISECONDS)) flush();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    private boolean release(Key key, Object voidOrThrowable) {
        Record record = map.remove(key);
        if (record == null) return false;

        flowControl.release();
        if (voidOrThrowable == Nils.VOID) record.callback.onSuccess(Nils.VOID);
        else record.callback.onFailure((Throwable) voidOrThrowable);
        return true;
    }

    protected class Record implements Comparable<Record> {
        private Long id;
        private Key key;
        private final V value;
        private final FutureCallback<Void> callback;

        public Record(long id, Key key, V value, FutureCallback<Void> callback) {
            this.id = id;
            this.key = key;
            this.value = value;
            this.callback = callback;
        }

        @Override
        public int compareTo(Record o) { return id.compareTo(o.id); }
    }

}
