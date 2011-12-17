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

package com.github.zhongl.kvengine;

import com.github.zhongl.ipage.Cursor;
import com.google.common.util.concurrent.FutureCallback;

/**
 * <pre>
 *  head |  skipping  |    collecting     | skipping <=> collecting | => head
 *       |@@@@@@@@@@@@|-------------------|.........................|
 * </pre>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
class AutoGarbageCollector<T> {
    private final AutoGarbageCollectable<T> collectable;
    private volatile boolean stopped;

    // TODO log info
    AutoGarbageCollector(AutoGarbageCollectable<T> collectable) {
        this.collectable = collectable;
    }

    public void start() {
        collectable.next(Cursor.<T>head(), new PreCollectingCallback());
    }

    public void stop() {
        stopped = true;
    }

    private class PreCollectingCallback implements FutureCallback<Cursor<T>> {
        private final long begin;

        public PreCollectingCallback(long begin) {
            this.begin = begin;
        }

        public PreCollectingCallback() {
            this(0L);
        }

        @Override
        public void onSuccess(Cursor<T> cursor) {
            if (stopped) return;
            if (cursor.isTail()) {
                collectable.next(Cursor.<T>head(), new PreCollectingCallback());
                return;
            }
            if (cursor.lastValue() == null) {
                collectable.next(cursor, new CollectingCallback(begin, cursor.offset()));
                return;
            }
            collectable.next(cursor, new PreCollectingCallback(cursor.offset()));
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
        }
    }

    private class CollectingCallback implements FutureCallback<Cursor<T>> {
        private final long begin;
        private final long end;

        public CollectingCallback(long begin, long end) {
            this.begin = begin;
            this.end = end;
        }

        @Override
        public void onSuccess(Cursor<T> cursor) {
            if (stopped) return;
            if (cursor.isTail()) {
                collectable.garbageCollect(begin, cursor.offset(), new CollectedCallback(Cursor.<T>head()));
                return;
            }
            if (cursor.lastValue() != null) {
                collectable.garbageCollect(begin, end, new CollectedCallback(cursor));
                return;
            }
            collectable.next(cursor, new CollectingCallback(begin, cursor.offset()));
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
        }
    }

    private class CollectedCallback implements FutureCallback<Long> {
        private final Cursor<T> cursor;

        public CollectedCallback(Cursor<T> cursor) {
            this.cursor = cursor;
        }

        @Override
        public void onSuccess(Long collectedLength) {
            if (stopped) return;
            collectable.next(cursor, new PreCollectingCallback(cursor.offset()));
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
        }
    }
}
