package com.github.zhongl.ipage;

import com.github.zhongl.util.CallByCountOrElapse;
import com.github.zhongl.util.Sync;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine extends Engine {

    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final IPage ipage;
    private final Index index;
    private final CallByCountOrElapse callByCountOrElapse;

    public KVEngine(long pollTimeout, int backlog, IPage ipage, Index index, CallByCountOrElapse callByCountOrElapse) {
        super(pollTimeout, DEFAULT_TIME_UNIT, backlog);
        this.ipage = ipage;
        this.index = index;
        this.callByCountOrElapse = callByCountOrElapse;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        Closeables.closeQuietly(index);
        Closeables.closeQuietly(ipage);
    }

    public static KVEngineBuilder baseOn(File dir) {
        return new KVEngineBuilder(dir);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean put(Md5Key key, Record record, FutureCallback<Record> callback) {
        return submit(new Put(key, record, callback));
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
        return submit(new Remove(key, callback));
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
            callByCountOrElapse.tryCallByElapse();
        } catch (Exception e) {
            e.printStackTrace();  // TODO right
        }
    }

    private void tryCallByCount() {
        try {
            callByCountOrElapse.tryCallByCount();
        } catch (Exception e) {
            e.printStackTrace();  // TODO right
        }
    }

    public abstract class Operation extends Task<Record> {

        protected final Md5Key key;

        public Operation(Md5Key key, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
        }

    }

    public class Put extends Operation {

        private final Record record;

        private Put(Md5Key key, Record record, FutureCallback<Record> callback) {
            super(key, callback);
            this.record = record;
        }

        @Override
        protected Record execute() throws IOException {
            Long offset = index.put(key, ipage.append(record));
            if (offset == null) return null;
            // TODO remove the old record
            tryCallByCount();
            return ipage.get(offset);
        }

    }

    public class Get extends Operation {

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

    public class Remove extends Operation {

        private Remove(Md5Key key, FutureCallback<Record> callback) {
            super(key, callback);
        }

        @Override
        protected Record execute() throws Throwable {
            Long offset = index.remove(key);
            // TODO use a slide window to async truncate iPage.
            if (offset == null) return null;
            Record record = ipage.get(offset);
            tryCallByCount();
            return record;
        }

    }
}
