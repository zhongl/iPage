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
import com.github.zhongl.util.Sync;
import com.google.common.collect.AbstractIterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class ValueIterator<T> extends AbstractIterator<T> {
    private Cursor cursor = Cursor.head();
    private final Nextable<Entry<T>> nextable;

    public ValueIterator(Nextable<Entry<T>> nextable) {
        this.nextable = nextable;
    }

    @Override
    protected T computeNext() {
        try {
            if (nextable.isTail(cursor)) return endOfData();
            ValueAndNextCursor<Entry<T>> valueAndNextCursor;
            for (; ; ) {
                Sync<ValueAndNextCursor<Entry<T>>> callback = new Sync<ValueAndNextCursor<Entry<T>>>();
                checkState(nextable.getAndNext(cursor, callback), "Too many tasks to submit.");
                valueAndNextCursor = callback.get();
                cursor = valueAndNextCursor.next();
                Entry<T> entry = valueAndNextCursor.value();
                if (entry != null) return entry.value();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
