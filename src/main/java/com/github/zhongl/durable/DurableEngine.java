/*
 * Copyright 2012 zhongl
 *
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

package com.github.zhongl.durable;

import com.github.zhongl.cache.Cache;
import com.github.zhongl.engine.Engine;
import com.github.zhongl.engine.Task;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.journal.Event;
import com.github.zhongl.journal.Events;
import com.github.zhongl.page.Page;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.sequence.OverflowException;
import com.github.zhongl.sequence.Sequence;
import com.github.zhongl.util.Sync;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class DurableEngine<T> extends Engine {
    private static final long TIMEOUT = Long.getLong("ipage.durable.engine.timeout.ms", 500L);
    private static final int BACKLOG = Integer.getInteger("ipage.durable.engine.backlog", 256);
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    private static final FutureCallback<Page<?>> NULL_CALLBACK = new FutureCallback<Page<?>>() {
        @Override
        public void onSuccess(Page<?> result) { }

        @Override
        public void onFailure(Throwable t) { }
    };

    private final Sequence<Entry<T>> sequence;
    private final Checkpoint checkpoint;
    private final LinkedList<Page<Event>> pendingPages;
    private final Events<Md5Key, T> events;
    private final Cache cache;
    private final Index index;
    private final AutoGarbageCollector<Entry<T>> collector;

    public DurableEngine(
            Sequence<Entry<T>> sequence,
            Index index,
            Checkpoint checkpoint,
            Events<Md5Key, T> events,
            Cache cache
    ) throws IOException {
        super(TIMEOUT, TIME_UNIT, BACKLOG);
        this.sequence = sequence;
        this.index = index;
        this.checkpoint = checkpoint;
        this.events = events;
        this.cache = cache;
        pendingPages = new LinkedList<Page<Event>>();
        collector = new AutoGarbageCollector<Entry<T>>(new InnerCollectable());
    }

    @Override
    public void startup() {
        super.startup();
        collector.start();
    }

    @Override
    public void shutdown() {
        collector.stop();
        super.shutdown();
    }

    public void apply(final Page<Event> page) {
        apply(page, NULL_CALLBACK);
    }

    @VisibleForTesting
    void apply(final Page<Event> page, FutureCallback<Page<?>> callback) {
        if (checkpoint.isApplied(page)) {
            page.clear();// clear alreay applied page , which may not clear successful last time because of crash.
            return;
        }

        pendingPages.add(page);

        submit(new Task<Page<?>>(new ApplyCallback(callback)) {
            @Override
            protected Page<?> execute() throws Throwable {
                for (Event event : page) {
                    if (events.isAdd(event)) add(event);
                    else delete(event);
                }
                return page;
            }
        });
    }

    private Cursor delete(Event event) throws IOException {
        return index.remove(events.getKey(event));
    }

    private Cursor add(final Event event) throws IOException, OverflowException {
        Md5Key key = events.getKey(event);
        T value = events.getValue(event);
        Cursor cursor = sequence.append(new Entry(key, value));
        index.put(key, cursor);
        return cursor;
    }

    public T load(final Md5Key key) throws IOException, InterruptedException {
        Sync<T> sync = new Sync<T>();
        submit(new Task<T>(sync) {
            @Override
            protected T execute() throws Throwable {
                return sequence.get(index.get(key)).value();
            }
        });
        return sync.get();
    }


    private class InnerCollectable implements Collectable<Entry<T>> {

        @Override
        public boolean get(final Cursor cursor, FutureCallback<Entry<T>> cursorFutureCallback) {
            return submit(new Task<Entry<T>>(cursorFutureCallback) {
                @Override
                protected Entry<T> execute() throws Throwable {
                    return sequence.get(cursor);
                }
            });
        }

        @Override
        public boolean garbageCollect(final Cursor begin, final Cursor end, FutureCallback<Long> longFutureCallback) {
            return submit(new Task<Long>(longFutureCallback) {
                @Override
                protected Long execute() throws Throwable {
                    return sequence.collect(begin, end);
                }
            });
        }

        @Override
        public boolean contains(Entry<T> object) {
            return cache.get(object.key()) == null;
        }

        @Override
        public Cursor calculateNextCursorBy(Cursor last, Entry<T> object) {
            return last.forword(sequence.byteLengthOf(object));
        }

        @Override
        public boolean isTail(Cursor cursor) {
            return sequence.tail().compareTo(cursor) <= 0;
        }
    }

    private class ApplyCallback implements FutureCallback<Page<?>> {

        private final FutureCallback<Page<?>> callback;

        public ApplyCallback(FutureCallback<Page<?>> callback) {
            this.callback = callback;
        }

        @Override
        public void onSuccess(Page<?> page) {
            try {
                if (checkpoint.canSave(sequence.tail())) { // clear applied pending pages
                    sequence.fixLastPage();
                    checkpoint.save(page.number(), sequence.tail());
                    sequence.addNewPage();

                    for (; ; ) {
                        Page<Event> removed = pendingPages.remove();
                        for (Event event : removed) {
                            if (events.isAdd(event)) cache.weak(events.getKey(event));
                        }
                        removed.clear();
                        if (removed == page) break;
                    }
                }
                callback.onSuccess(page);
            } catch (IOException e) {
                e.printStackTrace();
                callback.onFailure(e);
                // TODO log error
            }
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
            callback.onFailure(t);
            // TODO log error
        }

    }
}
