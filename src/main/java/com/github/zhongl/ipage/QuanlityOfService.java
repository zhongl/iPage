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

import com.github.zhongl.util.CallbackFuture;
import com.github.zhongl.util.FutureCallbacks;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Future;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class QuanlityOfService<K, V> {

    private final IPage<K, V> iPage;

    public QuanlityOfService(IPage<K, V> iPage) {this.iPage = iPage;}

    public void sendAdd(K key, V value) {
        iPage.add(key, value, FutureCallbacks.<Void>ignore());
    }

    public void sendRemove(K key) {
        iPage.remove(key, FutureCallbacks.<Void>ignore());
    }

    public void callAdd(K key, V value) {
        CallbackFuture<Void> callback = new CallbackFuture<Void>();
        iPage.add(key, value, callback);
        FutureCallbacks.getUnchecked(callback);
    }

    public void callRemove(K key) {
        CallbackFuture<Void> callback = new CallbackFuture<Void>();
        iPage.remove(key, callback);
        FutureCallbacks.getUnchecked(callback);
    }

    public Future<Void> futureAdd(K key, V value) {
        CallbackFuture<Void> callback = new CallbackFuture<Void>();
        iPage.add(key, value, callback);
        return callback;
    }

    public Future<Void> futureRemove(K key) {
        CallbackFuture<Void> callback = new CallbackFuture<Void>();
        iPage.remove(key, callback);
        return callback;
    }

    public V get(K key) {
        return iPage.get(key);
    }
}
