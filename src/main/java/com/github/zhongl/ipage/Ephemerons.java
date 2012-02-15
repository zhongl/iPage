/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.annotation.ManagedOperation;
import org.softee.management.annotation.Parameter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.github.zhongl.util.FutureCallbacks.getUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
@MBean
public abstract class Ephemerons<V> extends Actor {

    private final Map<Key, Record> map;
    private final FlushTask flushTask;
    private final Semaphore flowControl;
    private final Queue<Entry<Key, V>> incomingOrderQueue;

    private volatile boolean flushing;

    protected Ephemerons(String name) {
        super(name);
        map = new HashMap<Key, Record>();
        flowControl = new Semaphore(0, true);
        incomingOrderQueue = new LinkedList<Entry<Key, V>>();
        flushing = false;
        flushTask = new FlushTask();
    }

    @ManagedOperation
    public int throughout(@Parameter("delta") int delta) {
        if (delta > 0) flowControl.release(delta);
        if (delta < 0) flowControl.acquireUninterruptibly(-delta);
        return flowControl.availablePermits();
    }

    @ManagedAttribute
    public boolean isFlushing() {
        return flushing;
    }

    @ManagedAttribute
    public int getSize() {
        return getUnchecked(submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return map.size();
            }
        }));
    }

    public void add(final Key key, final V value, final FutureCallback<Void> removedOrDurableCallback) {
        checkNotNull(key);
        checkNotNull(value);
        checkNotNull(removedOrDurableCallback);

        acquire();
        submit(new AddTask(key, value, removedOrDurableCallback));
    }

    public void remove(final Key key, final FutureCallback<Void> appliedCallback) {
        checkNotNull(key);
        checkNotNull(appliedCallback);

        if (getUnchecked(submit(new TryReleasePhantomTask(key, appliedCallback)))) return;

        acquire();
        submit(new RemoveTask(key, appliedCallback));
    }

    public V get(final Key key) {
        checkNotNull(key);
        return getUnchecked(submit(new GetTask(key)));
    }

    public void flush() {
        submit(flushTask);
    }

    /** This method supposed be asynchronized. */
    protected abstract void requestFlush(Collection<Entry<Key, V>> appendings, Collection<Key> removings, FutureCallback<Void> flushedCallback);

    /** This method supposed be thread safed. */
    protected abstract V getMiss(Key key);

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
        if (voidOrThrowable == Nils.VOID)
            record.callback.onSuccess(Nils.VOID);
        else
            record.callback.onFailure((Throwable) voidOrThrowable);
        return true;
    }

    protected class Record {
        private final V value;
        private final FutureCallback<Void> callback;

        public Record(V value, FutureCallback<Void> callback) {
            this.value = value;
            this.callback = callback;
        }
    }

    private class AddTask implements Callable<Void> {
        private final Key key;
        private final V value;
        private final FutureCallback<Void> removedOrDurableCallback;

        public AddTask(Key key, V value, FutureCallback<Void> removedOrDurableCallback) {
            this.key = key;
            this.value = value;
            this.removedOrDurableCallback = removedOrDurableCallback;
        }

        @Override
        public Void call() throws Exception {
            release(key, Nils.VOID);
            incomingOrderQueue.add(new Entry<Key, V>(key, value));
            map.put(key, new Record(value, removedOrDurableCallback));
            return Nils.VOID;
        }
    }

    private class TryReleasePhantomTask implements Callable<Boolean> {
        private final Key key;
        private final FutureCallback<Void> appliedCallback;

        public TryReleasePhantomTask(Key key, FutureCallback<Void> appliedCallback) {
            this.key = key;
            this.appliedCallback = appliedCallback;
        }

        @Override
        public Boolean call() throws Exception {
            if (!release(key, Nils.VOID)) return false;
            appliedCallback.onSuccess(Nils.VOID);
            return true;
        }
    }

    private class RemoveTask implements Callable<Void> {
        private final Key key;
        private final FutureCallback<Void> appliedCallback;

        public RemoveTask(Key key, FutureCallback<Void> appliedCallback) {
            this.key = key;
            this.appliedCallback = appliedCallback;
        }

        @Override
        public Void call() throws Exception {
            incomingOrderQueue.add(new Entry<Key, V>(key, (V) Nils.OBJECT));
            map.put(key, new Record((V) Nils.OBJECT, appliedCallback));
            return Nils.VOID;
        }
    }

    private class GetTask implements Callable<V> {
        private final Key key;

        public GetTask(Key key) {this.key = key;}

        @Override
        public V call() throws Exception {
            Record record = map.get(key);
            if (record == null) return getMiss(key);
            if (record.value == Nils.OBJECT) return null;
            return record.value;
        }
    }

    private class FlushTask implements Callable<Void> {

        @Override
        public Void call() throws Exception {
            if (flushing || incomingOrderQueue.isEmpty()) return Nils.VOID;

            final Collection<Entry<Key, V>> appendings = new ArrayList<Entry<Key, V>>();
            final Collection<Key> removings = new ArrayList<Key>();

            while (true) {
                Entry<Key, V> entry = incomingOrderQueue.poll();
                if (entry == null) break;
                if (!map.containsKey(entry.key())) continue; // phantom key
                if (entry.value().equals(Nils.OBJECT)) removings.add(entry.key());
                else appendings.add(entry);
            }

            flushing = true;
            requestFlush(appendings, removings, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void v) {
                    foreachKey(new Function<Key, Void>() {
                        @Override
                        public Void apply(@Nullable Key key) {
                            release(key, Nils.VOID);
                            return Nils.VOID;
                        }

                    });
                }

                @Override
                public void onFailure(final Throwable t) {
                    foreachKey(new Function<Key, Void>() {
                        @Override
                        public Void apply(@Nullable Key key) {
                            release(key, t);
                            return Nils.VOID;
                        }
                    });
                }

                private void foreachKey(Function<Key, Void> function) {
                    for (Key key : removings) function.apply(key);
                    for (Entry<Key, V> entry : appendings) function.apply(entry.key());
                    flushing = false;
                }
            });

            return Nils.VOID;
        }
    }
}
