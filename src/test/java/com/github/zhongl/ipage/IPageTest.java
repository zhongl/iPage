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
import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.FutureCallbacks;
import com.github.zhongl.util.Md5;
import org.junit.Test;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Future;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");

        long flushMillis = 100L;
        int flushCount = 1000;
        IPage<String, String> iPage;
        int ephemeronThroughout = 1;
        iPage = new IPage<String, String>(dir, new StringCodec(), ephemeronThroughout, flushMillis, flushCount) {

            @Override
            protected Key transform(String key) {
                return new Key(Md5.md5(key.getBytes()));
            }
        };

        String value = "value";

        QuanlityOfService<String, String> service = new QuanlityOfService<String, String>(iPage);

        String key1 = "key1";
        service.sendAdd(key1, value);
        assertThat(iPage.get(key1), is(value));
        service.sendRemove(key1);
        assertThat(iPage.get(key1), is(nullValue()));

        String key2 = "key2";
        service.callAdd(key2, value);
        assertThat(iPage.get(key2), is(value));
        service.callRemove(key2);
        assertThat(iPage.get(key2), is(nullValue()));

        String key3 = "key3";
        service.futureAdd(key3, value).get();
        assertThat(iPage.get(key3), is(value));
        service.futureRemove(key3).get();
        assertThat(iPage.get(key3), is(nullValue()));

        iPage.stop();
    }

    /** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
    @ThreadSafe
    public static class QuanlityOfService<K, V> {

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

    }
}
