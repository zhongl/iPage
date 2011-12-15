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

import com.github.zhongl.buffer.Accessor;
import com.github.zhongl.builder.*;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.ipage.Cursor;
import com.github.zhongl.ipage.IPage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine<T> extends Engine {
    static final String IPAGE_DIR = "ipage";
    static final String INDEX_DIR = "index";

    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

    private final DataIntegerity dataIntegerity;
    private final Operation<T> operation;


    @VisibleForTesting
    KVEngine(long pollTimeout, int backlog, DataIntegerity dataIntegerity, Operation<T> operation) {
        super(pollTimeout, DEFAULT_TIME_UNIT, backlog);
        this.dataIntegerity = dataIntegerity;
        this.operation = operation;
    }

    KVEngine(int backlog,
             File dir,
             Accessor<T> valueAccessor,
             long maxChunkIdleTimeMillis,
             int maximizeChunkCapacity,
             long minimzieCollectLength,
             int initialBucketSize,
             int flushCount,
             long flushElapseMilliseconds,
             boolean groupCommit) {
        super(flushElapseMilliseconds / 2, DEFAULT_TIME_UNIT, backlog);

        // TODO refactor this method

        boolean exists = dir.exists();
        dir.mkdirs();
        Preconditions.checkArgument(dir.isDirectory(), "%s should be a directory", dir);

        final IPage<Entry<T>> iPage = IPage.<Entry<T>>baseOn(new File(dir, IPAGE_DIR))
                                           .maxChunkIdleTimeMillis(maxChunkIdleTimeMillis)
                                           .minimizeCollectLength(minimzieCollectLength)
                                           .accessor(new EntryAccessor<T>(valueAccessor))
                                           .maximizeChunkCapacity(maximizeChunkCapacity)
                                           .build();

        final Index index = Index.baseOn(new File(dir, INDEX_DIR)).initialBucketSize(initialBucketSize).build();

        dataIntegerity = new DataIntegerity(dir);

        if (exists && !dataIntegerity.validate()) new Recovery(index, iPage).run();

        CallByCountOrElapse callByCountOrElapse = new CallByCountOrElapse(flushCount, flushElapseMilliseconds, new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                iPage.flush();
                index.flush();
                return null;
            }
        });

        operation = new Operation<T>(iPage, index, groupCommit ? Group.newInstance() : Group.NULL, callByCountOrElapse);

    }

    @Override
    public void shutdown() throws InterruptedException {
        super.shutdown();
        try {
            operation.close();
            dataIntegerity.safeClose();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static <T> Builder<T> baseOn(File dir) {
        Builder<T> builder = Builders.newInstanceOf(Builder.class);
        builder.dir(dir);
        return builder;
    }

    // TODO @Count monitor
    // TODO @Elapse monitor
    public boolean put(Md5Key key, T value, FutureCallback<T> callback) {
        return submit(operation.put(key, value, callback));
    }

    public boolean get(Md5Key key, FutureCallback<T> callback) {
        return submit(operation.get(key, callback));
    }

    public boolean remove(Md5Key key, FutureCallback<T> callback) {
        return submit(operation.remove(key, callback));
    }

    public boolean next(Cursor<Entry<T>> entryCursor, FutureCallback<Cursor<Entry<T>>> callback) {
        return submit(operation.next(entryCursor, callback));
    }

    public boolean garbageCollect(long survivorOffset, FutureCallback<Long> callback) {
        return submit(operation.garbageCollect(survivorOffset, callback));
    }

    @Override
    protected void hearbeat() {
        operation.tryGroupCommitByElapse();
    }

    public static interface Builder<T> extends BuilderConvention {

        @OptionIndex(0)
        @DefaultValue("256")
        @GreaterThan("0")
        Builder<T> backlog(int value);

        @OptionIndex(1)
        @NotNull
        Builder<T> dir(File value);

        @OptionIndex(2)
        @NotNull
        Builder<T> valueAccessor(Accessor<T> value);

        @OptionIndex(3)
        @DefaultValue("4000")
        @GreaterThanOrEqual("1000")
        Builder<T> maxChunkIdleTimeMillis(long value);

        @OptionIndex(4)
        @DefaultValue("67108864") // 64M
        @GreaterThanOrEqual("4096")
        Builder<T> maximizeChunkCapacity(int value);

        @OptionIndex(5)
        @DefaultValue("67108864") // 64M
        @GreaterThanOrEqual("4096")
        Builder<T> minimzieCollectLength(long value);

        @OptionIndex(6)
        @DefaultValue("1024")
        @GreaterThan("0")
        Builder<T> initialBucketSize(int value);

        @OptionIndex(7)
        @DefaultValue("10000")
        @GreaterThan("0")
        Builder<T> flushCount(int value);

        @OptionIndex(8)
        @DefaultValue("1000")
        @GreaterThan("0")
        Builder<T> flushElapseMilliseconds(long value);

        @OptionIndex(9)
        @DefaultValue("false")
        Builder<T> groupCommit(boolean value);

        KVEngine<T> build();

    }
}
