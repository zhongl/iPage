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
import com.github.zhongl.ipage.IPage;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class KVEngineBuilder {

    static final long DEFAULT_FLUSH_ELAPSE_MILLISECONDS = 10L;
    static final int DEFAULT_BACKLOG = 10;
    static final int DEFAULT_FLUSH_COUNT = 100;
    static final String IPAGE_DIR = "ipage";
    static final String INDEX_DIR = "index";

    private static final int UNSET = -1;

    private final File dir;
    private int chunkCapacity = UNSET;
    private int initBucketSize = UNSET;
    private long flushElapseMilliseconds = UNSET;
    private int backlog = UNSET;
    private int flushCount = UNSET;
    private boolean groupCommit = false;

    public KVEngineBuilder(File dir) {
        this.dir = dir;
    }

    public KVEngineBuilder chunkCapacity(int value) {
        checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
//        Preconditions.checkArgument(value >= Chunk.DEFAULT_CAPACITY,
//                "Chunk capacity should not less than " + Chunk.DEFAULT_CAPACITY);
        chunkCapacity = value;
        return this;
    }

    public KVEngineBuilder backlog(int value) {
        checkState(backlog == UNSET, "Backlog can only set once.");
        checkArgument(value > 0, "Backlog should not less than 0");
        backlog = value;
        return this;
    }

    public KVEngineBuilder initialBucketSize(int value) {
        checkState(initBucketSize == UNSET, "Initial bucket size can only set once.");
        checkArgument(value > 0, "Initial bucket size should not less than 0");
        initBucketSize = value;
        return this;
    }

    public KVEngineBuilder flushByElapseMilliseconds(long value) {
        checkState(flushElapseMilliseconds == UNSET,
                "Flush elapse milliseconds can only set once.");
        checkArgument(value >= DEFAULT_FLUSH_ELAPSE_MILLISECONDS,
                "Flush elapse milliseconds should not less than " + DEFAULT_FLUSH_ELAPSE_MILLISECONDS);
        flushElapseMilliseconds = value;
        return this;
    }

    public KVEngineBuilder flushByCount(int value) {
        checkState(flushCount == UNSET, "Flush count can only set once.");
        checkArgument(value > 0, "Flush count should not less than 0");
        flushCount = value;
        return this;
    }

    public KVEngineBuilder groupCommit(boolean b) {
        groupCommit = b;
        return this;
    }

    public KVEngine build() throws IOException {
        DataSecurity dataSecurity = null;
        boolean exists = dir.exists();
        if (!exists) dir.mkdirs();
        dataSecurity = new DataSecurity(dir);

        final IPage ipage = newIPage();
        final Index index = newIndex();

        if (exists) {
            try {
                dataSecurity.validate();
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
        return new KVEngine(pollTimeout, backlog, group, ipage, index, callByCountOrElapse, dataSecurity);
    }

    private CallByCountOrElapse newCallFlushByCountOrElapse(Callable<Object> flusher) {
        flushCount = (flushCount == UNSET) ? DEFAULT_FLUSH_COUNT : flushCount;
        flushElapseMilliseconds =
                (flushElapseMilliseconds == UNSET) ?
                        DEFAULT_FLUSH_ELAPSE_MILLISECONDS : flushElapseMilliseconds;
        return new CallByCountOrElapse(flushCount, flushElapseMilliseconds, flusher);
    }

    private Index newIndex() throws IOException {
//        initBucketSize = (initBucketSize == UNSET) ? Buckets.DEFAULT_SIZE : initBucketSize;
        return Index.baseOn(new File(dir, INDEX_DIR)).initialBucketSize(initBucketSize).build();
    }

    private IPage newIPage() throws IOException {
//        chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
        return IPage.baseOn(new File(dir, IPAGE_DIR)).chunkCapacity(chunkCapacity).build();
    }
}
