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
import com.github.zhongl.ex.index.Index;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.ex.util.Entry;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import static com.github.zhongl.ex.actor.Actors.actor;
import static com.github.zhongl.ex.actor.Actors.call;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class DefaultBrowser extends Actor implements Browser, Updatable, Mergable {

    private final Map<Md5Key, byte[]> cache;
    private final Index index;

    public DefaultBrowser(Index index) {
        this.index = index;
        this.cache = new ConcurrentHashMap<Md5Key, byte[]>();
    }

    @Override
    public byte[] get(final Md5Key key) {
        byte[] bytes = cache.get(key);
        if (bytes == null) return get(index.get(key)); // cache miss
        return bytes == DefaultRecorder.NULL_VALUE ? null /*removed key*/ : bytes;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return call(new Function<FutureCallback<Iterator<byte[]>>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<Iterator<byte[]>> callback) {
                actor(Store.class).iterator(callback);
                return null;
            }
        });
    }

    @Override
    public void merge(final Iterator<Entry<Md5Key, Offset>> sortedIterator) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                index.merge(sortedIterator);
                while (sortedIterator.hasNext()) { // release cache
                    cache.remove(sortedIterator.next().key());
                }
                return null;
            }
        });
    }

    @Override
    public void update(final Entry<Md5Key, byte[]> entry) {
        submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return cache.put(entry.key(), entry.value());
            }
        });
    }

    @Override
    public void update(final Revision revision, final Entry<Md5Key, byte[]> entry) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (entry.value() == DefaultRecorder.NULL_VALUE) {
                    actor(Store.class).remove(revision, index.get(entry.key()));
                } else {
                    actor(Store.class).append(revision, entry);
                }
                return null;
            }
        });
    }

    private byte[] get(final Offset offset) {
        return call(new Function<FutureCallback<byte[]>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<byte[]> callback) {
                actor(Store.class).get(offset, callback);
                return null;
            }
        });
    }
}
