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

import com.github.zhongl.sequence.Cursor;
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
    private final Collectable<T> collectable;
    private volatile boolean stopped;

    // TODO log info
    AutoGarbageCollector(Collectable<T> collectable) {
        this.collectable = collectable;
    }

    public void start() {
        collectable.get(Cursor.head(), new PreCollectingCallback());
    }

    public void stop() {
        stopped = true;
    }

    private class PreCollectingCallback implements FutureCallback<T> {
        private final Cursor last;

        public PreCollectingCallback(Cursor last) {
            this.last = last;
        }

        public PreCollectingCallback() {
            this(Cursor.head());
        }

        @Override
        public void onSuccess(T entry) {
            if (stopped) return;
            Cursor next = collectable.calculateNextCursorBy(last, entry);
            if (collectable.isTail(last)) {
                collectable.get(Cursor.head(), new PreCollectingCallback());
                return;
            }
            if (!collectable.contains(entry)) {
                if (collectable.isTail(next)) {
                    collectable.garbageCollect(last, next, new CollectedCallback(Cursor.head()));
                } else {
                    collectable.get(next, new CollectingCallback(last, next));
                }
                return;
            }
            collectable.get(next, new PreCollectingCallback(next));
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
        }
    }

    private class CollectingCallback implements FutureCallback<T> {
        private final Cursor begin;
        private final Cursor last;

        public CollectingCallback(Cursor begin, Cursor last) {
            this.begin = begin;
            this.last = last;
        }

        @Override
        public void onSuccess(T entry) {
            if (stopped) return;
            Cursor next = collectable.calculateNextCursorBy(last, entry);
            if (collectable.contains(entry)) {
                collectable.garbageCollect(begin, last, new CollectedCallback(next));
                return;
            }
            if (collectable.isTail(next)) {
                collectable.garbageCollect(begin, next, new CollectedCallback(Cursor.head()));
                return;
            }
            collectable.get(next, new CollectingCallback(begin, next));
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
        }
    }

    private class CollectedCallback implements FutureCallback<Long> {
        private final Cursor cursor;

        public CollectedCallback(Cursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public void onSuccess(Long collectedLength) {
            if (stopped) return;
            collectable.get(cursor, new PreCollectingCallback(cursor));
        }

        @Override
        public void onFailure(Throwable t) {
            t.printStackTrace();
        }
    }
}
