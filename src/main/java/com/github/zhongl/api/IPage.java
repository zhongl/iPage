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

package com.github.zhongl.api;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.index.Key;
import com.github.zhongl.util.CallByCountOrElapse;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.helper.MBeanRegistration;
import org.softee.management.helper.ObjectNameBuilder;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public abstract class IPage<K, V> extends Actor implements Iterable<V> {

    private static final String EPHEMERONS = "Ephemerons";
    private static final String STORAGE = "Storage";
    private static final String DEFRAG_POLICY = "DefragPolicy";

    private final Storage<V> storage;
    private final Ephemerons<V> ephemerons;
    private final CallByCountOrElapse callByCountOrElapse;
    private final DefragPolicy defragPolicy;

    protected IPage(File dir, Codec<V> valueCodec, int throughout, long flushMillis, int flushCount) throws Exception {
        super("IPage@" + dir.getName(), flushMillis / 2);

        Md5KeyCodec keyCodec = new Md5KeyCodec();
        RangeIndexCodec rangeIndexCodec = new RangeIndexCodec(keyCodec);
        Snapshot<V> snapshot = new Snapshot<V>(dir, rangeIndexCodec, new EntryCodec<V>(keyCodec, valueCodec));

        defragPolicy = new DefragPolicy();
        this.storage = new Storage<V>(snapshot, rangeIndexCodec, defragPolicy);
        this.ephemerons = new Ephemerons<V>() {
            @Override
            protected void requestFlush(
                    final Collection<WriteOperation<Entry<Key, V>>> addOrUpdates,
                    final Collection<WriteOperation<Key>> removes,
                    final FutureCallback<Void> flushedCallback
            ) {
                submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        storage.merge(addOrUpdates, removes, flushedCallback);
                        return Nils.VOID;
                    }
                });
            }

            @Override
            protected V getMiss(Key key) { return storage.get(key); }
        };

        ephemerons.throughout(throughout);

        this.callByCountOrElapse = new CallByCountOrElapse(flushCount, flushMillis, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (!ephemerons.isFlushing()) ephemerons.flush();
                return Nils.VOID;
            }
        });

        new MBeanRegistration(ephemerons, objectName(EPHEMERONS)).register();
        new MBeanRegistration(storage, objectName(STORAGE)).register();
        new MBeanRegistration(defragPolicy, objectName(DEFRAG_POLICY)).register();
    }

    public void add(final K key, final V value, FutureCallback<Void> removedOrDurableCallback) {
        ephemerons.add(transform(key), value, removedOrDurableCallback);
        tryCallByCount();
    }

    public void remove(K key, FutureCallback<Void> appliedCallback) {
        ephemerons.remove(transform(key), appliedCallback);
        tryCallByCount();
    }

    public V get(K key) { return ephemerons.get(transform(key)); }

    @Override
    public Iterator<V> iterator() { return storage.iterator(); }

    @Override
    public void stop() {
        super.stop();
        try {
            new MBeanRegistration(ephemerons, objectName(EPHEMERONS)).unregister();
            new MBeanRegistration(storage, objectName(STORAGE)).unregister();
            new MBeanRegistration(defragPolicy, objectName(DEFRAG_POLICY)).unregister();
        } catch (Exception ignored) { }
    }

    @Override
    protected void heartbeat() throws Throwable { callByCountOrElapse.tryCallByElapse(); }

    @Override
    protected boolean onInterruptedBy(Throwable t) {
        t.printStackTrace();
        return super.onInterruptedBy(t);    // TODO log error
    }

    protected abstract Md5Key transform(K key);

    private void tryCallByCount() {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                callByCountOrElapse.tryCallByCount();
                return Nils.VOID;
            }
        });
    }

    private ObjectName objectName(String type) throws MalformedObjectNameException {
        return new ObjectNameBuilder("com.github.zhongl.ipage").withType(type).withName(toString()).build();
    }
}
