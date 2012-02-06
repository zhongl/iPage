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

import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.nio.Forcer;
import com.github.zhongl.ex.util.Tuple;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

import static com.github.zhongl.ex.nio.ByteBuffers.lengthOf;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class DefaultBatch extends Batch {
    protected final int start;
    protected final int estimateBufferSize;
    protected final Queue<Tuple> delayTransformQueue;
    protected final Kit kit;

    private final Queue<Tuple> tupleQueue;

    public DefaultBatch(Kit kit, int start, int estimateBufferSize) {
        this.kit = kit;
        this.start = start;
        this.estimateBufferSize = Math.max(4096, estimateBufferSize);
        this.delayTransformQueue = new LinkedList<Tuple>();
        this.tupleQueue = new LinkedList<Tuple>();
    }

    @Override
    protected Cursor _append(Object object) {
        return null;  // TODO _append
    }

    @Override
    protected final int _writeAndForceTo(FileChannel channel) {
        try {
            int size = Forcer.getInstance().force(channel, aggregate());

            while (!delayTransformQueue.isEmpty()) {
                Tuple tuple = delayTransformQueue.poll();
                FutureCallback<Cursor> callback = tuple.get(0);
                Integer offset = tuple.get(1);
                callback.onSuccess(kit.cursor(offset));
            }

            return size;
        } catch (Throwable t) {
            while (!delayTransformQueue.isEmpty()) {
                Tuple tuple = delayTransformQueue.poll();
                FutureCallback<Cursor> callback = tuple.get(0);
                callback.onFailure(t);
            }

            // FIXME
            throw new IllegalStateException("Data may be inconsistent cause by force failed", t);
        }
    }

    @Override
    protected void _append(Object value, FutureCallback<Cursor> callback) {
        tupleQueue.offer(new Tuple(kit.encode(value), callback));
    }

    private ByteBuffer aggregate() throws IOException {
        int position = start;
        ByteBuffer aggregated = ByteBuffer.allocate(estimateBufferSize);

        for (Tuple tuple : toAggregatingQueue()) {
            final ByteBuffer buffer = tuple.get(0);
            final FutureCallback<Cursor> callback = tuple.get(1);

            delayTransformQueue.offer(new Tuple(callback, position));

            position = position + lengthOf(buffer);
            aggregated = ByteBuffers.aggregate(aggregated, buffer);
        }

        return (ByteBuffer) aggregated.flip();
    }

    protected Iterable<Tuple> toAggregatingQueue() { return tupleQueue; }
}
