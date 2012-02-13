package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class IPage<K, V> extends Actor implements Iterable<V> {

    private final Storage<V> storage;
    private final Ephemerons<V> ephemerons;
    private final QuanlityOfService quanlityOfService;

    public IPage(File dir, QuanlityOfService quanlityOfService, Codec<V> codec) throws IOException {
        this.quanlityOfService = quanlityOfService;
        storage = new Storage<V>(dir, codec);

        ephemerons = new Ephemerons<V>(new ConcurrentHashMap<Key, Ephemerons<V>.Record>()) {
            @Override
            protected void requestFlush(
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

            @Override
            protected V getMiss(Key key) {
                return storage.get(key);
            }

        };
    }

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
                ephemerons.add(transform(key), value, removedOrDurableCallback);
                return null;
            }
        });
    }

    public void remove(K key) {
        ephemerons.remove(transform(key));
    }

    public V get(K key) {
        return ephemerons.get(transform(key));
    }

    @Override
    public Iterator<V> iterator() {
        return storage.iterator();
    }

    protected abstract Key transform(K key);
}
