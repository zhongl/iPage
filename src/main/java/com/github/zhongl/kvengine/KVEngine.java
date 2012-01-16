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

import com.github.zhongl.builder.*;
import com.github.zhongl.nio.Accessor;
import com.google.common.util.concurrent.FutureCallback;
import org.iq80.leveldb.*;
import org.iq80.leveldb.impl.Iq80DBFactory;
import org.iq80.leveldb.impl.WriteBatchImpl;
import org.iq80.leveldb.util.Slice;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine extends Engine {

    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final DB db;
    private final Group group;
    private final CallByCountOrElapse callByCountOrElapse;
    private final Map<byte[], byte[]> cache;
    private volatile WriteBatch writeBatch;

    KVEngine(int backlog,
             File dir,
             Accessor<?> valueAccessor,
             long maxChunkIdleTimeMillis,
             int maximizeChunkCapacity,
             long minimzieCollectLength,
             int initialBucketSize,
             int flushCount,
             long flushElapseMilliseconds,
             boolean groupCommit,
             boolean startAutoGarbageCollectOnStartup) {

        super(flushElapseMilliseconds / 2, DEFAULT_TIME_UNIT, backlog);

        Options option = new Options();
        cache = new ConcurrentHashMap<byte[], byte[]>();
        try {
            db = new Iq80DBFactory().open(dir, option);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        group = groupCommit ? Group.newInstance() : Group.NULL;
        callByCountOrElapse = new CallByCountOrElapse(flushCount, flushElapseMilliseconds, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                db.write(writeBatch, new WriteOptions().sync(true));

                ((WriteBatchImpl) writeBatch).forEach(new WriteBatchImpl.Handler() {
                    @Override
                    public void put(Slice key, Slice value) {
                        cache.remove(key.getBytes());
                    }

                    @Override
                    public void delete(Slice key) { }
                });

                writeBatch = db.createWriteBatch();

                return null;  // TODO call
            }
        });
        writeBatch = db.createWriteBatch();
    }

    @Override
    public void startup() {
        super.startup();
    }

    @Override
    public void shutdown() throws InterruptedException {
        super.shutdown();
        cache.clear();
        db.close();
    }

    private void tryGroupCommitByElapse() {
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

    public static Builder baseOn(File dir) {
        Builder builder = Builders.newInstanceOf(Builder.class);
        builder.dir(dir);
        return builder;
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean put(final Md5Key key, final byte[] value, FutureCallback<byte[]> callback) {
        return submit(new Task<byte[]>(group.decorate(callback)) {
            @Override
            protected byte[] execute() throws Throwable {
                group.register(callback);
                //                byte[] prevalue = get(key);
                cache.put(key.toBytes(), value);
                writeBatch.put(key.toBytes(), value);
                tryGroupCommitByCount();
                //                return prevalue;
                return null;
            }
        });
    }

    public boolean get(final Md5Key key, FutureCallback<byte[]> callback) {
        return submit(new Task<byte[]>(callback) {
            @Override
            protected byte[] execute() throws Throwable {
                return get(key);
            }
        });
    }

    private byte[] get(Md5Key key) {
        byte[] value = cache.get(key.toBytes());
        if (value != null) return value;
        return db.get(key.toBytes());
    }

    public boolean remove(final Md5Key key, FutureCallback<byte[]> callback) {
        return submit(new Task<byte[]>(group.decorate(callback)) {
            @Override
            protected byte[] execute() throws Throwable {
                group.register(callback);
                byte[] value = get(key);
                cache.remove(key.toBytes());
                writeBatch.delete(key.toBytes());
                tryGroupCommitByCount();
                return value;
            }
        });
    }

    @Override
    protected void hearbeat() { tryGroupCommitByElapse(); }

    public synchronized Iterator<byte[]> iterator() {
        final DBIterator iterator = db.iterator();
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public byte[] next() {
                return iterator.next().getValue();
            }

            @Override
            public void remove() { }
        };
    }

    public static interface Builder extends BuilderConvention {

        @ArgumentIndex(0)
        @DefaultValue("256")
        @GreaterThan("0")
        Builder backlog(int value);

        @ArgumentIndex(1)
        @NotNull
        Builder dir(File value);

        @ArgumentIndex(2)
        @NotNull
        Builder valueAccessor(Accessor value);

        @ArgumentIndex(3)
        @DefaultValue("4000")
        @GreaterThanOrEqual("1000")
        Builder maxChunkIdleTimeMillis(long value);

        @ArgumentIndex(4)
        @DefaultValue("67108864") // 64M
        @GreaterThanOrEqual("4096")
        Builder maximizeChunkCapacity(int value);

        @ArgumentIndex(5)
        @DefaultValue("67108864") // 64M
        @GreaterThanOrEqual("4096")
        Builder minimzieCollectLength(long value);

        @ArgumentIndex(6)
        @DefaultValue("1024")
        @GreaterThan("0")
        Builder initialBucketSize(int value);

        @ArgumentIndex(7)
        @DefaultValue("10000")
        @GreaterThan("0")
        Builder flushCount(int value);

        @ArgumentIndex(8)
        @DefaultValue("1000")
        @GreaterThan("0")
        Builder flushElapseMilliseconds(long value);

        @ArgumentIndex(9)
        @DefaultValue("false")
        Builder groupCommit(boolean value);

        @ArgumentIndex(10)
        @DefaultValue("false")
        Builder startAutoGarbageCollectOnStartup(boolean value);

        KVEngine build();

    }
}
