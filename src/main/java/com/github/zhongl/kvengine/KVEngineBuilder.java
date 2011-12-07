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

import com.github.zhongl.accessor.Accessor;
import com.github.zhongl.index.FileHashTable;
import com.github.zhongl.index.Index;
import com.github.zhongl.ipage.Chunk;
import com.github.zhongl.ipage.IPage;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class KVEngineBuilder<V> {

    static final long DEFAULT_FLUSH_ELAPSE_MILLISECONDS = 10L;
    static final int DEFAULT_BACKLOG = 10;
    static final int DEFAULT_FLUSH_COUNT = 100;
    static final String IPAGE_DIR = "ipage";
    static final String INDEX_DIR = "index";

    private static final int UNSET = -1;

    private final File dir;
    private int chunkCapacity = UNSET;
    private int initialBucketSize = UNSET;
    private long flushElapseMilliseconds = UNSET;
    private int backlog = UNSET;
    private int flushCount = UNSET;
    private boolean groupCommit = false;
    private Accessor<V> valueAccessor;

    public KVEngineBuilder(File dir) {
        this.dir = dir;
    }

    public KVEngineBuilder<V> chunkCapacity(int value) {
        checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
        checkArgument(value >= Chunk.DEFAULT_CAPACITY,
                "Chunk capacity should not less than " + Chunk.DEFAULT_CAPACITY);
        chunkCapacity = value;
        return this;
    }

    public KVEngineBuilder<V> valueAccessor(Accessor<V> accessor) {
        checkState(valueAccessor == null, "Value accessor can only set once.");
        checkNotNull(accessor, "Value accessor should not be null");
        this.valueAccessor = accessor;
        return this;
    }

    public KVEngineBuilder<V> backlog(int value) {
        checkState(backlog == UNSET, "Backlog can only set once.");
        checkArgument(value > 0, "Backlog should not less than 0");
        backlog = value;
        return this;
    }

    public KVEngineBuilder<V> initialBucketSize(int value) {
        checkState(initialBucketSize == UNSET, "Initial bucket buckets can only set once.");
        checkArgument(value > 0, "Initial bucket buckets should not less than 0");
        initialBucketSize = value;
        return this;
    }

    public KVEngineBuilder<V> flushByElapseMilliseconds(long value) {
        checkState(flushElapseMilliseconds == UNSET,
                "Flush elapse milliseconds can only set once.");
        checkArgument(value >= DEFAULT_FLUSH_ELAPSE_MILLISECONDS,
                "Flush elapse milliseconds should not less than " + DEFAULT_FLUSH_ELAPSE_MILLISECONDS);
        flushElapseMilliseconds = value;
        return this;
    }

    public KVEngineBuilder<V> flushByCount(int value) {
        checkState(flushCount == UNSET, "Flush count can only set once.");
        checkArgument(value > 0, "Flush count should not less than 0");
        flushCount = value;
        return this;
    }

    public KVEngineBuilder<V> groupCommit(boolean b) {
        groupCommit = b;
        return this;
    }

    public KVEngine<V> build() throws IOException {
        boolean exists = dir.exists();
        if (!exists) dir.mkdirs();

        DataIntegerity dataIntegerity = new DataIntegerity(dir);

        final IPage<Entry<V>> ipage = newIPage();
        final Index index = newIndex();

        if (exists) {
            try {
                dataIntegerity.validate();
            } catch (UnsafeDataStateException e) {
                new Recovery(index, ipage).run();
            }
        }

        CallByCountOrElapse callByCountOrElapse = newCallFlushByCountOrElapse(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                ipage.flush();
                index.flush();
                return null;
            }
        });

        long pollTimeout = flushElapseMilliseconds / 2; // smaller poll timeout can guarantee accuration of flushing time.
        backlog = (backlog == UNSET) ? DEFAULT_BACKLOG : backlog;
        Group group = groupCommit ? Group.newInstance() : Group.NULL;

        return new KVEngine<V>(pollTimeout, backlog, group, ipage, index, callByCountOrElapse, dataIntegerity);
    }

    private CallByCountOrElapse newCallFlushByCountOrElapse(Callable<Object> flusher) {
        flushCount = (flushCount == UNSET) ? DEFAULT_FLUSH_COUNT : flushCount;
        flushElapseMilliseconds =
                (flushElapseMilliseconds == UNSET) ?
                        DEFAULT_FLUSH_ELAPSE_MILLISECONDS : flushElapseMilliseconds;
        return new CallByCountOrElapse(flushCount, flushElapseMilliseconds, flusher);
    }

    private Index newIndex() throws IOException {
        initialBucketSize = (initialBucketSize == UNSET) ? FileHashTable.DEFAULT_SIZE : initialBucketSize;
        return Index.baseOn(new File(dir, INDEX_DIR)).initialBucketSize(initialBucketSize).build();
    }

    private IPage<Entry<V>> newIPage() throws IOException {
        checkNotNull(valueAccessor, "Value accessor need to set.");
        chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
        return IPage.<Entry<V>>baseOn(new File(dir, IPAGE_DIR))
                .accessor(new EntryAccessor<V>(valueAccessor))
                .chunkCapacity(chunkCapacity).build();
    }
}
