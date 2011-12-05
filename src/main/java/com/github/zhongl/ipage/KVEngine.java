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

package com.github.zhongl.ipage;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine extends Engine {

    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final IPage ipage;
    private final Index index;
    private final CallByCountOrElapse callByCountOrElapse;
    private final DataSecurity dataSecurity;
    private final Group group;

    public KVEngine(long pollTimeout, int backlog, Group group, IPage ipage, Index index, CallByCountOrElapse callByCountOrElapse, DataSecurity dataSecurity) {
        super(pollTimeout, DEFAULT_TIME_UNIT, backlog);
        this.group = group;
        this.ipage = ipage;
        this.index = index;
        this.callByCountOrElapse = callByCountOrElapse;
        this.dataSecurity = dataSecurity;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        try {
            index.close();
            ipage.close();
            dataSecurity.safeClose();
        } catch (IOException e) {
            e.printStackTrace();  // TODO log e
        }
    }

    public static KVEngineBuilder baseOn(File dir) {
        return new KVEngineBuilder(dir);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean put(Md5Key key, Record record, FutureCallback<Record> callback) {
        return submit(new Put(key, record, group.decorate(callback)));
    }

    public Record put(Md5Key key, Record record) throws IOException, InterruptedException {
        Sync<Record> callback = new Sync<Record>();
        put(key, record, callback);
        return callback.get();
    }

    public boolean get(Md5Key key, FutureCallback<Record> callback) {
        return submit(new Get(key, callback));
    }

    public Record get(Md5Key key) throws IOException, InterruptedException {
        Sync<Record> callback = new Sync<Record>();
        get(key, callback);
        return callback.get();
    }

    public boolean remove(Md5Key key, FutureCallback<Record> callback) {
        return submit(new Remove(key, group.decorate(callback)));
    }

    public Record remove(Md5Key key) throws IOException, InterruptedException {
        Sync<Record> callback = new Sync<Record>();
        remove(key, callback);
        return callback.get();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("KVEngine");
        sb.append("{ipage=").append(ipage);
        sb.append(", index=").append(index);
        sb.append('}');
        return sb.toString();
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

    public abstract class Operation extends Task<Record> {

        protected final Md5Key key;

        public Operation(Md5Key key, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
        }

    }

    private class Put extends Operation {

        private final Record record;

        private Put(Md5Key key, Record record, FutureCallback<Record> callback) {
            super(key, callback);
            this.record = record;
        }

        @Override
        protected Record execute() throws IOException {
            Long offset = index.put(key, ipage.append(record));
            group.register(callback);
            tryCallByCount();
            if (offset == null) return null;
            // TODO remove the old record
            return ipage.get(offset);
        }
    }

    private class Get extends Operation {

        private Get(Md5Key key, FutureCallback<Record> callback) {
            super(key, callback);
        }

        @Override
        protected Record execute() throws Throwable {
            Long offset = index.get(key);
            if (offset == null) return null;
            return ipage.get(offset);
        }
    }

    private class Remove extends Operation {

        private Remove(Md5Key key, FutureCallback<Record> callback) {
            super(key, callback);
        }

        @Override
        protected Record execute() throws Throwable {
            Long offset = index.remove(key);
            group.register(callback);
            tryCallByCount();
            // TODO use a slide window to async truncate iPage.
            if (offset == null) return null;
            Record record = ipage.get(offset);
            return record;
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
