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
import com.github.zhongl.ex.journal.CheckpointKeeper;
import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.Entry;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;

import static com.github.zhongl.ex.actor.Actors.actor;
import static com.github.zhongl.ex.util.FutureCallbacks.call;
import static com.github.zhongl.ex.util.FutureCallbacks.getUnchecked;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class DefaultBrowser extends Actor implements Browser, Updatable, Mergable {

    public static final Object PHANTOM = new Object();
    public static final Object REMOVED = new Object();

    private final Index index;
    private final CheckpointKeeper checkpointKeeper;
    private final SortedMap<Md5Key, Object> cache;

    @GuardedBy("this actor")
    private final Queue<Entry<Md5Key, Future<Cursor>>> removings;
    @GuardedBy("this actor")
    private final Queue<Entry<Md5Key, byte[]>> appendings;

    public DefaultBrowser(Index index, CheckpointKeeper checkpointKeeper) {
        this.index = index;
        this.checkpointKeeper = checkpointKeeper;
        this.cache = new ConcurrentSkipListMap<Md5Key, Object>();
        this.appendings = new LinkedList<Entry<Md5Key, byte[]>>();
        this.removings = new LinkedList<Entry<Md5Key, Future<Cursor>>>();
    }

    @Override
    public byte[] get(final Md5Key key) {
        Object value = cache.get(key);
        if (value == null) return get(index.get(key)); // cache miss
        if (value instanceof byte[]) return (byte[]) value;
        return null; // removed or phantom key
    }

    @Override
    public void merge(final Iterable<Entry<Md5Key, Cursor>> sortedIterable, final Checkpoint checkpoint) {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                index.merge(sortedIterable.iterator());
                for (Entry<Md5Key, Cursor> entry : sortedIterable) cache.remove(entry.key()); // release cache
                checkpointKeeper.last(checkpoint);
                actor(Erasable.class).erase(checkpoint);
                return null;
            }
        });
    }

    @Override
    public void update(final Entry<Md5Key, byte[]> entry) {
        final Object value;

        if (entry.value().length > 0) {
            value = entry.value(); // new entry
            appendings.add(entry);
        } else if (cache.containsKey(entry.key())) {
            appendings.remove(new Entry<Md5Key, byte[]>(entry.key(), (byte[]) cache.get(entry.key())));
            value = PHANTOM;
        } else {
            Future<Cursor> future = submit(new Callable<Cursor>() {
                @Override
                public Cursor call() throws Exception {
                    return index.get(entry.key());
                }
            });
            value = REMOVED;
            // IMPROVE use parallel
            removings.add(new Entry<Md5Key, Future<Cursor>>(entry.key(), future));
        }

        cache.put(entry.key(), value);
    }

    @Override
    public void force(final Checkpoint checkpoint) {
        if (checkpoint.compareTo(checkpointKeeper.last()) <= 0) return;
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (appendings.isEmpty() && removings.isEmpty()) actor(Erasable.class).erase(checkpoint);

                Iterator<Entry<Md5Key, Cursor>> removingsItr = new AbstractIterator<Entry<Md5Key, Cursor>>() {
                    Iterator<Entry<Md5Key, Future<Cursor>>> itr = copyAndClear(removings).iterator();

                    @Override
                    protected Entry<Md5Key, Cursor> computeNext() {
                        if (!itr.hasNext()) return endOfData();
                        Entry<Md5Key, Future<Cursor>> entry = itr.next();
                        return new Entry<Md5Key, Cursor>(entry.key(), getUnchecked(entry.value()));
                    }
                };

                actor(Durable.class).merge(copyAndClear(appendings).iterator(), removingsItr, checkpoint);

                return null;
            }
        });
    }

    private byte[] get(final Cursor cursor) {
        if (cursor == null) return null;
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
