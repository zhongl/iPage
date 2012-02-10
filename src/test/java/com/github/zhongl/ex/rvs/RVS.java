package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RVS {


}

class IPage<K, V> {
    Storage<K, V> storage;

    QuanlityOfService quanlityOfService;

    Ephemerons<K, V> ephemerons = new Ephemerons<K, V>(new ConcurrentHashMap<K, Ephemerons<K, V>.Record>()) {
        @Override
        protected V getMiss(K key) {
            return storage.get(key);
        }

        @Override
        protected void requestFlush(SortedSet<Entry<K, V>> entries, FutureCallback<Void> flushedCallback) {
            storage.merge(entries, flushedCallback);
        }
    };

    public void add(final K key, final V value) throws Throwable {
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

enum QuanlityOfService {
    RELIABLE {
        @Override
        public void call(Function<FutureCallback<Void>, Void> function) throws Throwable {
            CallbackFuture<Void> future = new CallbackFuture<Void>();
            function.apply(future);
            try {
                future.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }
    }, LATENCY {
        @Override
        public void call(Function<FutureCallback<Void>, Void> function) {
            function.apply(FutureCallbacks.<Void>ignore());
        }
    };

    public abstract void call(Function<FutureCallback<Void>, Void> function) throws Throwable;
}


