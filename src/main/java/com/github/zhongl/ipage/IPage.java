package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPage<K extends Comparable<K>, V> {
    Storage<K, V> storage;

    QuanlityOfService quanlityOfService;

    Ephemerons<K, V> ephemerons = new Ephemerons<K, V>(new ConcurrentHashMap<K, Ephemerons<K, V>.Record>()) {
        @Override
        protected void requestFlush(
                Collection<Entry<K, V>> appendings,
                Collection<K> removings,
                FutureCallback<Void> flushedCallback
        ) {
            storage.merge(appendings, removings, flushedCallback);
        }

        @Override
        protected V getMiss(K key) {
            return storage.get(key);
        }

    };

    /**
     * @param key
     * @param value
     *
     * @throws IllegalStateException cause by {@link QuanlityOfService#RELIABLE}
     */
    public void add(final K key, final V value) {
        quanlityOfService.call(new Function<FutureCallback<Void>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<Void> removedOrDurableCallback) {
                ephemerons.add(key, value, removedOrDurableCallback);
                return null;
            }
        });
    }

    public void remove(K key) {ephemerons.remove(key);}

    public V get(K key) {return ephemerons.get(key);}

}
