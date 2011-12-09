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

import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.ipage.Cursor;
import com.github.zhongl.ipage.IPage;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine<T> extends Engine {

    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final IPage<Entry<T>> iPage;
    private final Index index;
    private final CallByCountOrElapse callByCountOrElapse;
    private final DataIntegerity dataIntegerity;
    private final Group group;

    public KVEngine(long pollTimeout,
                    int backlog,
                    Group group,
                    IPage<Entry<T>> iPage,
                    Index index,
                    CallByCountOrElapse callByCountOrElapse,
                    DataIntegerity dataIntegerity
    ) {
        super(pollTimeout, DEFAULT_TIME_UNIT, backlog);
        this.group = group;
        this.iPage = iPage;
        this.index = index;
        this.callByCountOrElapse = callByCountOrElapse;
        this.dataIntegerity = dataIntegerity;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            index.close();
            iPage.close();
            dataIntegerity.safeClose();
        } catch (IOException e) {
            e.printStackTrace();  // TODO log e
        }
    }

    public static <V> KVEngineBuilder<V> baseOn(File dir) {
        return new KVEngineBuilder<V>(dir);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean put(Md5Key key, T value, FutureCallback<T> callback) {
        return submit(new Put(key, value, group.decorate(callback)));
    }

    public T put(Md5Key key, T value) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        put(key, value, callback);
        return callback.get();
    }

    public boolean get(Md5Key key, FutureCallback<T> callback) {
        return submit(new Get(key, callback));
    }

    public T get(Md5Key key) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        get(key, callback);
        return callback.get();
    }

    public boolean remove(Md5Key key, FutureCallback<T> callback) {
        return submit(new Remove(key, group.decorate(callback)));
    }

    public T remove(Md5Key key) throws IOException, InterruptedException {
        Sync<T> callback = new Sync<T>();
        remove(key, callback);
        return callback.get();
    }

    @Override
    protected void hearbeat() {
        try {
            if (callByCountOrElapse.tryCallByElapse()) group.commit();
        } catch (Throwable t) {
            group.rollback(t);
            t.printStackTrace();  // TODO log
        }
    }

    private void tryCallByCount() {
        try {
            if (callByCountOrElapse.tryCallByCount()) group.commit();
        } catch (Throwable t) {
            group.rollback(t);
            t.printStackTrace();  // TODO log
        }
    }


    public Iterator<T> valueIterator() {
        return new AbstractIterator<T>() {
            private Cursor<Entry<T>> cursor = Cursor.begin(-1L);

            @Override
            protected T computeNext() {
                try {
                    Sync<Cursor<Entry<T>>> callback = new Sync<Cursor<Entry<T>>>();
                    submit(new Next(cursor, callback));
                    cursor = callback.get();
                    if (cursor.isEnd()) return endOfData();
                    if (cursor.lastValue() == null) return computeNext();
                    return cursor.lastValue().value();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        };
    }

    public abstract class Operation extends Task<T> {

        protected final Md5Key key;

        public Operation(Md5Key key, FutureCallback<T> callback) {
            super(callback);
            this.key = key;
        }

    }

    private class Put extends Operation {

        private final T value;

        private Put(Md5Key key, T value, FutureCallback<T> callback) {
            super(key, callback);
            this.value = value;
        }

        @Override
        protected T execute() throws IOException {
            Long offset = index.put(key, iPage.append(new Entry<T>(key, value)));
            group.register(callback);
            tryCallByCount();
            if (offset == null) return null;
            // TODO remove the old lastValue
            return iPage.get(offset).value();
        }
    }

    private class Get extends Operation {

        private Get(Md5Key key, FutureCallback<T> callback) {
            super(key, callback);
        }

        @Override
        protected T execute() throws Throwable {
            Long offset = index.get(key);
            if (offset == null) return null;
            return iPage.get(offset).value();
        }
    }

    private class Remove extends Operation {

        private Remove(Md5Key key, FutureCallback<T> callback) {
            super(key, callback);
        }

        @Override
        protected T execute() throws Throwable {
            Long offset = index.remove(key);
            group.register(callback);
            tryCallByCount();
            // TODO use a slide window to async truncate iPage.
            if (offset == null) return null;
            return iPage.get(offset).value();
        }

    }

    private class Next extends Engine.Task<Cursor<Entry<T>>> {

        private final Cursor<Entry<T>> cursor;

        public Next(Cursor<Entry<T>> cursor, FutureCallback<Cursor<Entry<T>>> callback) {
            super(callback);
            this.cursor = cursor;
        }

        @Override
        protected Cursor<Entry<T>> execute() throws Throwable {
            Cursor<Entry<T>> next = iPage.next(cursor);
            if (next.isEnd()) return next; // EOF
            if (index.contains(next.lastValue().key()))
                return next;
            return Cursor.cursor(next.offset(), null); // value was deleted
        }
    }


    private static class Sync<T> implements FutureCallback<T> {

        private final CountDownLatch latch = new CountDownLatch(1);
        private T result;
        private Throwable t;

        @Override
        public void onSuccess(T result) {
            this.result = result;
            latch.countDown();
        }

        public T get() throws IOException, InterruptedException {
            latch.await();
            Throwables.propagateIfPossible(t, IOException.class); // cast IOException and throw
            if (t != null) Throwables.propagate(t); // cast RuntimeException Or Error and throw
            return result;
        }

        @Override
        public void onFailure(Throwable t) {
            this.t = t;
            latch.countDown();
        }
    }
}
