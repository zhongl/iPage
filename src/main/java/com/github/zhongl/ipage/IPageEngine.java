package com.github.zhongl.ipage;

import com.github.zhongl.util.Sync;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class IPageEngine extends Engine {

    static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
    static final String IPAGE_DIR = "ipage";
    static final String INDEX_DIR = "index";

    private final File dir;
    private final int flushCount;
    private final IPage ipage;
    private final Index index;

    private volatile int count = 0;

    public IPageEngine(
            final File dir,
            int chunkCapacity,
            int initBucketSize,
            int backlog,
            long flushIntervalMilliseconds,
            int flushCount) throws IOException {
        super(flushIntervalMilliseconds, DEFAULT_TIME_UNIT, backlog);
        this.dir = dir;
        this.flushCount = flushCount;
        ipage = IPage.baseOn(new File(dir, IPAGE_DIR)).chunkCapacity(chunkCapacity).build();
        index = Index.baseOn(new File(dir, INDEX_DIR)).initBucketSize(initBucketSize).build();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        Closeables.closeQuietly(index);
        Closeables.closeQuietly(ipage);
    }

    public static Builder baseOn(File dir) {
        return new Builder(dir);
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean append(Record record, FutureCallback<Md5Key> callback) {
        return submit(new Append(record, callback));
    }

    public Md5Key append(Record record) throws IOException, InterruptedException {
        Sync<Md5Key> callback = new Sync<Md5Key>();
        append(record, callback);
        return callback.get();
    }

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
        sb.append("IPageEngine");
        sb.append("{dir=").append(dir);
        sb.append(", com.github.zhongl.ipage=").append(ipage);
        sb.append(", index=").append(index);
        sb.append('}');
        return sb.toString();
    }

    @Override
    protected void onTick() {
        // TODO log tick flush
        flush();
    }

    private void flush() {
        try {
            ipage.flush();
            index.flush();
            resetCount();
            resetTick();
        } catch (IOException e) {
            e.printStackTrace();  // TODO log
        }
    }

    private void tryFlushByCount() {
        if (++count == flushCount) {
            // TODO log count flush
            flush();
        }
    }

    private void resetCount() {count = 0;}

    private class Append extends Task<Md5Key> {

        private final Record record;

        public Append(Record record, FutureCallback<Md5Key> callback) {
            super(callback);
            this.record = record;
        }

        @Override
        protected Md5Key execute() throws IOException {
            Md5Key key = Md5Key.valueOf(record);
            index.put(key, ipage.append(record));
            tryFlushByCount();
            return key;
        }
    }

    private class Put extends Task<Record> {

        private final Md5Key key;
        private final Record record;

        public Put(Md5Key key, Record record, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
            this.record = record;
        }

        @Override
        protected Record execute() throws IOException {
            Long offset = index.put(key, ipage.append(record));
            if (offset == null) return null;
            tryFlushByCount();
            return ipage.get(offset);
        }
    }

    private class Get extends Task<Record> {

        private final Md5Key key;

        public Get(Md5Key key, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
        }

        @Override
        protected Record execute() throws Throwable {
            Long offset = index.get(key);
            if (offset == null) return null;
            return ipage.get(offset);
        }

    }

    private class Remove extends Task<Record> {

        private final Md5Key key;

        public Remove(Md5Key key, FutureCallback<Record> callback) {
            super(callback);
            this.key = key;
        }

        @Override
        protected Record execute() throws Throwable {
            Long offset = index.remove(key);
            // TODO use a slide window to async truncate iPage.
            if (offset == null) return null;
            Record record = ipage.get(offset);
            tryFlushByCount();
            return record;
        }

    }

    public static class Builder {

        private static final int UNSET = -1;
        private static final long DEFAULT_FLUSH_INTERVAL_MILLISECONDS = 10L;
        private static final int DEFAULT_BACKLOG = 10;
        private static final int DEFAULT_FLUSH_COUNT = 100;

        private final File dir;
        private int chunkCapacity = UNSET;
        private int initBucketSize = UNSET;
        private long flushIntervalMilliseconds = UNSET;
        private int backlog = UNSET;
        private int flushCount = UNSET;

        public Builder(File dir) {
            this.dir = dir;
        }

        public Builder chunkCapacity(int value) {
            checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
            chunkCapacity = value;
            return this;
        }

        public Builder backlog(int value) {
            checkState(backlog == UNSET, "Backlog can only set once.");
            backlog = value;
            return this;
        }

        public Builder initBucketSize(int value) {
            checkState(initBucketSize == UNSET, "Initial bucket size can only set once.");
            initBucketSize = value;
            return this;
        }

        public Builder flushByIntervalMilliseconds(long value) {
            checkState(flushIntervalMilliseconds == UNSET, "Flush interval milliseconds can only set once.");
            flushIntervalMilliseconds = value;
            return this;
        }

        public Builder flushByCount(int value) {
            checkState(flushCount == UNSET, "Flush count can only set once.");
            flushCount = value;
            return this;
        }

        public IPageEngine build() throws IOException {
            chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
            initBucketSize = (initBucketSize == UNSET) ? Buckets.DEFAULT_SIZE : initBucketSize;
            backlog = (backlog == UNSET) ? DEFAULT_BACKLOG : backlog;
            flushCount = (flushCount == UNSET) ? DEFAULT_FLUSH_COUNT : flushCount;
            flushIntervalMilliseconds =
                    (flushIntervalMilliseconds == UNSET) ?
                            DEFAULT_FLUSH_INTERVAL_MILLISECONDS : flushIntervalMilliseconds;
            return new IPageEngine(dir, chunkCapacity, initBucketSize, backlog, flushIntervalMilliseconds, flushCount);
        }
    }
}
