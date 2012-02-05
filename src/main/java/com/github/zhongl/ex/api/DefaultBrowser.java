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

package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.actor.Actors;
import com.github.zhongl.ex.index.Index;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class DefaultBrowser extends Actor implements Browser, Updatable, Mergable {

    private Map<Md5Key, byte[]> cache;
    private Index index;

    @Override
    public byte[] get(Md5Key key) {
        byte[] bytes = cache.get(key);
        if (bytes == null) return get(index.get(key)); // cache miss
        if (bytes.length == 0) return null; // removed key
        return bytes;
    }

    private byte[] get(final Offset offset) {
        return Actors.sync(new Function<FutureCallback<byte[]>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<byte[]> callback) {
                Actors.actor(Store.class).get(offset, callback);
                return null;  // TODO apply
            }
        });
    }

    @Override
    public Iterator<byte[]> iterator() {
        return Actors.sync(new Function<FutureCallback<Iterator<byte[]>>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<Iterator<byte[]>> callback) {
                Actors.actor(Store.class).iterator(callback);
                return null;
            }
        });
    }

    @Override
    public void update(Future<Revision> future, Entry<Md5Key, byte[]> entry) {

        // TODO update
    }

    @Override
    public void merge(final Iterator<Entry<Md5Key, Offset>> sortedIterator) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                index.merge(sortedIterator);
                return null;
            }
        });
    }
}
