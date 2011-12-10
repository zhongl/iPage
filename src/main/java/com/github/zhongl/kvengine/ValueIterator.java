/*
 * Copyright 2011 zhongl
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
import com.google.common.collect.AbstractIterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class ValueIterator<T> extends AbstractIterator<T> {
    private Cursor<Entry<T>> cursor = Cursor.begin(-1L);
    private final KVEngine<T> engine;

    public ValueIterator(KVEngine<T> engine) {
        this.engine = engine;
    }

    @Override
    protected T computeNext() {
        try {
            Sync<Cursor<Entry<T>>> callback = new Sync<Cursor<Entry<T>>>();
            checkState(engine.next(cursor, callback), "Too many tasks to submit.");
            cursor = callback.get();
            if (cursor.isEnd()) return endOfData();
            if (cursor.lastValue() == null) return computeNext();
            return cursor.lastValue().value();
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
