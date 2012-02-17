package com.github.zhongl.ipage;

import com.github.zhongl.util.CallbackFuture;
import com.github.zhongl.util.FutureCallbacks;

import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngine<K, V> {
    private final IPage<K, V> iPage;
    private QoS qoS;

    public KVEngine(IPage<K, V> iPage, QoS qoS) {
        this.iPage = iPage;
        this.qoS = qoS;
    }

    public void add(K key, V value) { qoS.add(iPage, key, value); }

    public void remove(K key) { qoS.remove(iPage, key); }

    public V get(K key) { return iPage.get(key); }

    public void stop() { iPage.stop(); }

    public Iterator<V> iterator() { return iPage.iterator(); }

    public static enum QoS {
        LATENCY {
            @Override
            <K, V> void add(IPage<K, V> iPage, K key, V value) {
                iPage.add(key, value, FutureCallbacks.<Void>ignore());
            }
        }, RELIABLE {
            @Override
            <K, V> void add(IPage<K, V> iPage, K key, V value) {
                CallbackFuture<Void> callback = new CallbackFuture<Void>();
                iPage.add(key, value, callback);
                FutureCallbacks.getUnchecked(callback);
            }

        };

        abstract <K, V> void add(IPage<K, V> iPage, K key, V value);

        <K, V> void remove(IPage<K, V> iPage, K key) {
            iPage.remove(key, FutureCallbacks.<Void>ignore());
        }
    }
}
