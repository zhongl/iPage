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
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.Entry;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.LinkedList;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.github.zhongl.ex.actor.Actors.actor;
import static com.github.zhongl.ex.util.FutureCallbacks.call;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class DefaultBrowser extends Actor implements Browser, Updatable, Mergable {

    public static final Object PHANTOM = new Object();

    private final Index index;
    private final SortedMap<Md5Key, Object> cache;

    @GuardedBy("this actor")
    private final Queue<Entry<Md5Key, Cursor>> removings;
    @GuardedBy("this actor")
    private final Queue<Entry<Md5Key, byte[]>> appendings;

    public DefaultBrowser(Index index) {
        this.index = index;
        this.cache = new ConcurrentSkipListMap<Md5Key, Object>();
        this.appendings = new LinkedList<Entry<Md5Key, byte[]>>();
        this.removings = new LinkedList<Entry<Md5Key, Cursor>>();
    }

    @Override
    public byte[] get(final Md5Key key) {
        Object value = cache.get(key);
        if (value == null) return get(index.get(key)); // cache miss
        if (value instanceof byte[]) return (byte[]) value;
        return null; // removed or phantom key
    }

    @Override
    public void merge(final Iterable<Entry<Md5Key, Cursor>> sortedIterable) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                index.merge(sortedIterable.iterator());
                for (Entry<Md5Key, Cursor> entry : sortedIterable) cache.remove(entry.key()); // release cache
                return null;
            }
        });
    }

    @Override
    public void update(final Entry<Md5Key, byte[]> entry) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Object value;

                if (entry.value().length > 0) {
                    value = entry.value(); // new entry
                    appendings.add(entry);
                } else if (cache.containsKey(entry.key())) {
                    value = PHANTOM; // phantom entry
                } else {
                    value = index.get(entry.key()); // removed entry
                    // IMPROVE use parallel
                    removings.add(new Entry<Md5Key, Cursor>(entry.key(), (Cursor) value));
                }

                cache.put(entry.key(), value);
                return null;
            }
        });
    }

    @Override
    public void force(final Checkpoint checkpoint) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Queue<Entry<Md5Key, byte[]>> appendingsCopy = copyAndClear(appendings);
                Queue<Entry<Md5Key, Cursor>> removingsCopy = copyAndClear(removings);
                actor(Durable.class).merge(appendingsCopy.iterator(), removingsCopy.iterator(), checkpoint);

                return null;
            }
        });
    }

    private byte[] get(final Cursor cursor) {
        return call(new Function<FutureCallback<byte[]>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<byte[]> callback) {
                actor(Durable.class).get(cursor, callback);
                return null;
            }
        });
    }

    private static <T> Queue<T> copyAndClear(Queue<T> queue) {
        Queue<T> copy = new LinkedList<T>(queue);
        queue.clear();
        return copy;
    }
}
