/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ex.page;

import com.github.zhongl.ex.nio.Forcer;
import com.github.zhongl.ex.util.Tuple;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class AbstractBatch<V> implements Batch<V> {

    private final Queue<Tuple> tupleCycle;
    private final int estimateBufferSize;

    private boolean notWrote = true;

    protected AbstractBatch(int estimateBufferSize) {
        this.tupleCycle = new LinkedList<Tuple>();
        this.estimateBufferSize = Math.max(4096, estimateBufferSize);
    }

    @Override
    public void append(V value, FutureCallback<Cursor> forceCallback) {
        checkState(notWrote);
        tupleCycle.offer(tuple(checkNotNull(forceCallback), checkNotNull(value)));
    }

    @Override
    public int writeAndForceTo(FileChannel channel) {
        notWrote = false;
        int size = 0;
        Throwable error = null;

        try {
            size = Forcer.getInstance().force(channel, aggregate());
        } catch (Throwable t) {
            error = t;
        }

        while (!tupleCycle.isEmpty()) callback(tupleCycle.poll(), error);

        if (error != null) // FIXME a better way to deal with error
            throw new IllegalStateException("Data may be inconsistent cause by force failed", error);

        return size;
    }


    protected abstract Tuple tuple(FutureCallback<Cursor> callback, V value);

    protected abstract Tuple aggregate(Tuple tuple, ByteBuffer aggregated);

    protected abstract void callback(Tuple tuple, Throwable error);

    private ByteBuffer aggregate() throws IOException {
        ByteBuffer aggregated = ByteBuffer.allocate(estimateBufferSize);

        for (int i = 0; i < tupleCycle.size(); i++) {
            tupleCycle.offer(aggregate(tupleCycle.poll(), aggregated));
        }

        return (ByteBuffer) aggregated.flip();
    }

}
