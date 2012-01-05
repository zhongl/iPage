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

package com.github.zhongl.kvengine;

import com.github.zhongl.builder.*;
import com.github.zhongl.cache.Cache;
import com.github.zhongl.cache.Durable;
import com.github.zhongl.durable.Checkpoint;
import com.github.zhongl.durable.DurableEngine;
import com.github.zhongl.durable.Entry;
import com.github.zhongl.durable.EntryAccessor;
import com.github.zhongl.index.Index;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.journal.Journal;
import com.github.zhongl.page.Accessor;
import com.github.zhongl.sequence.Cursor;
import com.github.zhongl.sequence.Sequence;
import com.github.zhongl.sequence.SequenceLoader;
import com.github.zhongl.sequence.UnderflowException;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class KVEngine<T> {

    private final DurableEngine<T> durableEngine;
    private final Cache<Md5Key, T> cache;
    private final Journal journal;
    private final Md5KeyEvents<T> events;

    KVEngine(
            File dir,
            Accessor<T> vAccessor,
            int flushCount,
            long flushElapseMillisenconds,
            long minimizeCollectLength,
            boolean groupCommit,
            int initialBucketSize,
            int groupApplyLength,
            int cacheCapacity,
            long durationMilliseconds,
            boolean autoStartGC
    ) throws IOException {

        Accessor<Entry<T>> accessor = new EntryAccessor<T>(vAccessor);
        events = new Md5KeyEvents<T>(vAccessor);

        Checkpoint checkpoint = new Checkpoint(new File(dir, ".cp"), groupApplyLength);
        Cursor lastSequenceTail = checkpoint.lastCursor();

        SequenceLoader<Entry<T>> loader = new SequenceLoader<Entry<T>>(new File(dir, "seq"), accessor, lastSequenceTail);
        final Sequence<Entry<T>> sequence = new Sequence<Entry<T>>(loader, minimizeCollectLength);

        Index index = new Index(new File(dir, "idx"), initialBucketSize);

        index.validateOrRecoverBy(new com.github.zhongl.index.Validator() {
            @Override
            public boolean validate(Md5Key key, Cursor cursor) throws IOException {
                try {
                    return sequence.get(cursor).key().equals(key);
                } catch (UnderflowException e) {
                    return false;
                }
            }
        });


        durableEngine = new DurableEngine<T>(sequence, index, checkpoint, events, autoStartGC);
        Durable<Md5Key, T> durable = new Durable<Md5Key, T>() {
            @Override
            public T load(Md5Key key) throws IOException, InterruptedException {
                return durableEngine.load(key);
            }
        };

        cache = new Cache<Md5Key, T>(events, durable, cacheCapacity, durationMilliseconds);

        journal = new Journal(new File(dir, "jou"), events, durableEngine, cache, flushCount, flushElapseMillisenconds, groupCommit);
    }


    public void startup() {
        try {
            durableEngine.startup();
            journal.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void shutdown() {
        try {
            journal.close();
            durableEngine.shutdown();
            cache.cleanUp();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <T> Builder<T> baseOn(File dir) {
        Builder<T> builder = Builders.newInstanceOf(Builder.class);
        builder.dir(dir);
        return builder;
    }

    public boolean put(Md5Key key, T value, FutureCallback<T> callback) {
        return journal.append(events.put(key, value, callback, cache.get(key)));
    }

    public boolean get(Md5Key key, FutureCallback<T> callback) {
        callback.onSuccess(cache.get(key));
        return true;
    }

    public boolean remove(Md5Key key, FutureCallback<T> callback) {
        return journal.append(events.remove(key, callback, cache.get(key)));
    }

    public Iterator<T> valueIterator() { return durableEngine.valueIterator(); }

    public void startAutoGarbageCollect() { durableEngine.startAutoGarbageCollect(); }

    public void stopAutoGarbageCollect() { durableEngine.stopAutoGarbageCollect(); }

    public static interface Builder<T> extends BuilderConvention {


        @ArgumentIndex(0)
        @NotNull
        Builder<T> dir(File value);

        @ArgumentIndex(1)
        @NotNull
        Builder<T> valueAccessor(Accessor<T> value);

        @ArgumentIndex(2)
        @DefaultValue("10000")
        @GreaterThan("0")
        Builder<T> flushCount(int value);

        @ArgumentIndex(3)
        @DefaultValue("1000")
        @GreaterThan("0")
        Builder<T> flushElapseMilliseconds(long value);

        @ArgumentIndex(4)
        @DefaultValue("67108864") // 64M
        @GreaterThanOrEqual("4096")
        Builder<T> minimzieCollectLength(long value);

        @ArgumentIndex(5)
        @DefaultValue("false")
        Builder<T> groupCommit(boolean value);

        @ArgumentIndex(6)
        @DefaultValue("1024")
        @GreaterThan("0")
        Builder<T> initialBucketSize(int value);

        @ArgumentIndex(7)
        @DefaultValue("67108864") // 64M
        @GreaterThanOrEqual("4096")
        Builder<T> groupApplyLength(int value);

        @ArgumentIndex(8)
        @DefaultValue("5000")
        @GreaterThan("0")
        Builder<T> cacheCapacity(int value);

        @ArgumentIndex(9)
        @DefaultValue("1000")
        @GreaterThan("0")
        Builder<T> durationMilliseconds(long value);

        @ArgumentIndex(10)
        @DefaultValue("false")
        Builder<T> autoStartGC(boolean value);

        KVEngine<T> build();
    }

}
