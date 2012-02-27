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
        iPage.start();
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
