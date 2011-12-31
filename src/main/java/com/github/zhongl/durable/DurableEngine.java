/*
 * Copyright 2011 zhongl
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

import com.github.zhongl.cache.Durable;
import com.github.zhongl.engine.Engine;
import com.github.zhongl.engine.Task;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.journal.Event;
import com.github.zhongl.journal.Events;
import com.github.zhongl.kvengine.Sync;
import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.Page;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.sequence.OverflowException;
import com.github.zhongl.sequence.Sequence;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class DurableEngine<T> extends Engine implements Durable<Md5Key, T> {
    private static final long TIMEOUT = Long.getLong("ipage.durable.engine.timeout.ms", 500L);
    private static final int BACKLOG = Integer.getInteger("ipage.durable.engine.backlog", 256);
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final Sequence<Entry<T>> sequence;
    private final Checkpoint checkpoint;
    private final LinkedList<Page<Event>> pendingPages;
    private final Events<Md5Key, T> events;
    private final Index index;


    public DurableEngine(
            File dir,
            Accessor<T> accessor,
            Events<Md5Key, T> events,
            int groupApplyLength,
            long minimizeCollectLength,
            int initialBucketSize) throws IOException {
        super(TIMEOUT, TIME_UNIT, BACKLOG);
        this.events = events;
        pendingPages = new LinkedList<Page<Event>>();
        checkpoint = new Checkpoint(new File(dir, ".cp"), groupApplyLength);
        // TODO clean checkpoint for recovery
        index = new Index(new File(dir, "idx"), initialBucketSize);
        sequence = new Sequence<Entry<T>>(new File(dir, "seq"), new EntryAccessor<T>(accessor), groupApplyLength, minimizeCollectLength);

    }

    @Override
    public void startup() {
        super.startup();
        // TODO startup auto gc
    }

    @Override
    public void shutdown() {
        // TODO shutdown auto gc
        super.shutdown();
    }

    public void apply(final Page<Event> page) {
        if (checkpoint.isApplied(page)) {
            page.clear();// clear alreay applied page , which may not clear successful last time because of crash.
            return;
        }

        pendingPages.add(page);

        submit(new Task<Cursor>(new ApplyCallback(page)) {
            @Override
            protected Cursor execute() throws Throwable {
                Cursor cursor = null;
                for (Event event : page) {
                    cursor = events.isAdd(event) ? add(event) : delete(event);
                }
                return cursor;
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

    @Override
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

    private class ApplyCallback implements FutureCallback<Cursor> {

        private final Page<Event> page;

        public ApplyCallback(Page<Event> page) {
            this.page = page;
        }

        @Override
        public void onSuccess(Cursor cursor) {
            try {
                if (checkpoint.trySaveBy(cursor, page.number())) { // clear applied pending pages
                    while (pendingPages.peek() != page)
                        pendingPages.remove().clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
                // TODO log error
            }
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
            // TODO log error
        }
    }
}
