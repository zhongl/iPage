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

import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.ipage.Cursor;
import com.github.zhongl.ipage.IPage;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
class Operation<T> implements Closeable {

    private final IPage<Entry<T>> iPage;
    private final Index index;
    private final Group group;
    private final CallByCountOrElapse callByCountOrElapse;

    public Operation(IPage<Entry<T>> iPage, Index index, Group group, CallByCountOrElapse callByCountOrElapse) {
        this.iPage = iPage;
        this.index = index;
        this.group = group;
        this.callByCountOrElapse = callByCountOrElapse;
    }

    @Override
    public void close() throws IOException {
        index.close();
        iPage.close();
    }

    public Task<T> put(final Md5Key key, final T value, FutureCallback<T> callback) {
        return new Task<T>(group.decorate(callback)) {

            @Override
            protected T execute() throws IOException {
                Long offset = index.put(key, iPage.append(new Entry<T>(key, value)));
                group.register(callback);
                tryGroupCommitByCount();
                if (offset == null) return null;
                return iPage.get(offset).value();
            }
        };
    }

    public Task<T> get(final Md5Key key, FutureCallback<T> callback) {
        return new Task<T>(callback) {
            @Override
            protected T execute() throws Throwable {
                Long offset = index.get(key);
                if (offset == null) return null;
                return iPage.get(offset).value();
            }
        };
    }

    public Task<T> remove(final Md5Key key, FutureCallback<T> callback) {
        return new Task<T>(group.decorate(callback)) {
            @Override
            protected T execute() throws Throwable {
                Long offset = index.remove(key);
                group.register(callback);
                tryGroupCommitByCount();
                if (offset == null) return null;
                return iPage.get(offset).value();
            }
        };
    }

    public Task<Cursor<Entry<T>>> next(final Cursor<Entry<T>> entryCursor, FutureCallback<Cursor<Entry<T>>> callback) {
        return new Task<Cursor<Entry<T>>>(callback) {

            @Override
            protected Cursor<Entry<T>> execute() throws Throwable {
                Cursor<Entry<T>> next = iPage.next(entryCursor);
                if (next.isEnd()) return next; // EOF
                if (index.contains(next.lastValue().key()))
                    return next;
                return Cursor.cursor(next.offset(), null); // value was deleted
            }
        };
    }

    public Task<Long> garbageCollect(final long survivorOffset, FutureCallback<Long> callback) {
        return new Task<Long>(callback) {

            @Override
            protected Long execute() throws Throwable {
                return iPage.garbageCollect(survivorOffset);
            }
        };
    }

    public void tryGroupCommitByElapse() {
        try {
            if (callByCountOrElapse.tryCallByElapse()) group.commit();
        } catch (Throwable t) {
            group.rollback(t);
            t.printStackTrace();  // TODO log
        }
    }

    private void tryGroupCommitByCount() {
        try {
            if (callByCountOrElapse.tryCallByCount()) group.commit();
        } catch (Throwable t) {
            group.rollback(t);
            t.printStackTrace();  // TODO log
        }
    }

}
