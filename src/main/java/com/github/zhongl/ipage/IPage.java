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

import com.github.zhongl.util.CallByCountOrElapse;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.helper.MBeanRegistration;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public abstract class IPage<K, V> extends Actor implements Iterable<V> {

    private static final String EPHEMERONS = "Ephemerons";
    private static final String STORAGE = "Storage";

    private final Storage<V> storage;
    private final Ephemerons<V> ephemerons;
    private final CallByCountOrElapse callByCountOrElapse;

    public IPage(File dir,
                 Codec<V> codec,
                 int ephemeronThroughout,
                 long flushMillis,
                 int flushCount) throws Exception {

        super("IPage", (flushMillis / 2));
        this.storage = new Storage<V>(dir, codec);
        this.ephemerons = new Ephemerons<V>("Ephemerons") {
            @Override
            protected void requestFlush(
                    final Collection<Entry<Key, V>> appendings,
                    final Collection<Key> removings,
                    final FutureCallback<Void> flushedCallback
            ) {
                merge(appendings, removings, flushedCallback);
            }

            @Override
            protected V getMiss(Key key) {
                return storage.get(key);
            }

        };

        ephemerons.throughout(ephemeronThroughout);

        this.callByCountOrElapse = new CallByCountOrElapse(flushCount, flushMillis, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ephemerons.flush();
                return Nils.VOID;
            }
        });

        new MBeanRegistration(ephemerons, objectName(EPHEMERONS)).register();
        new MBeanRegistration(storage, objectName(STORAGE)).register();
    }

    private void merge(
            final Collection<Entry<Key, V>> appendings,
            final Collection<Key> removings,
            final FutureCallback<Void> flushedCallback
    ) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                storage.merge(appendings, removings, flushedCallback);
                return Nils.VOID;
            }
        });
    }

    public void add(final K key, final V value, FutureCallback<Void> removedOrDurableCallback) {
        ephemerons.add(transform(key), value, removedOrDurableCallback);
        tryCallByCount();
    }

    public void remove(K key, FutureCallback<Void> appliedCallback) {
        ephemerons.remove(transform(key), appliedCallback);
        tryCallByCount();
    }

    public V get(K key) {
        return ephemerons.get(transform(key));
    }

    @Override
    public Iterator<V> iterator() {
        return storage.iterator();
    }

    @Override
    public void stop() {
        ephemerons.stop();
        super.stop();
        try {
            new MBeanRegistration(ephemerons, objectName(EPHEMERONS)).unregister();
            new MBeanRegistration(storage, objectName(STORAGE)).unregister();
        } catch (Exception ignored) { }
    }

    @Override
    protected void heartbeat() throws Throwable {
        callByCountOrElapse.tryCallByElapse();
    }

    @Override
    protected boolean onInterruptedBy(Throwable t) {
        return super.onInterruptedBy(t);    // TODO log error
    }

    protected abstract Key transform(K key);

    private void tryCallByCount() {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                callByCountOrElapse.tryCallByCount();
                return Nils.VOID;
            }
        });
    }

    private ObjectName objectName(String ephemerons1) throws MalformedObjectNameException {
        return new ObjectName(MessageFormat.format("com.github.zhongl.ipage:type={0},belongs={1}", ephemerons1, this));
    }
}
