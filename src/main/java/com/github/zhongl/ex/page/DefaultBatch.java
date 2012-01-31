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

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Tuple;
import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.nio.Forcer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

import static com.github.zhongl.ex.nio.ByteBuffers.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class DefaultBatch extends Batch {
    protected final Queue<Runnable> delayTransformQueue;
    private final Queue<Tuple> tupleQueue;

    public DefaultBatch(final File file, int position, final Codec codec, int estimateBufferSize) {
        super(file, position, codec, estimateBufferSize);
        this.delayTransformQueue = new LinkedList<Runnable>();
        this.tupleQueue = new LinkedList<Tuple>();
    }

    @Override
    protected final <T> Cursor<T> _append(T object) {
        final ObjectRef<T> objectRef = new ObjectRef<T>(object, codec);
        final Transformer<T> transformer = new Transformer<T>(objectRef);
        onAppend(objectRef, transformer);
        return transformer;
    }

    protected void onAppend(final ObjectRef<?> objectRef, final Transformer<?> transformer) {
        tupleQueue.offer(new Tuple(transformer, objectRef.encode()));
    }

    @Override
    protected final int _writeAndForceTo(FileChannel channel) throws IOException {
        int size = Forcer.getInstance().force(channel, aggregate());
        while (!delayTransformQueue.isEmpty())
            delayTransformQueue.poll().run();
        return size;
    }

    private ByteBuffer aggregate() throws IOException {
        int offset = position;
        ByteBuffer aggregated = ByteBuffer.allocate(estimateBufferSize);

        for (Tuple tuple : toAggregatingQueue()) {
            final Transformer<?> transformer = tuple.get(0);
            final ByteBuffer buffer = tuple.get(1);
            final Cursor<Object> reader = new Reader<Object>(file, offset, codec);

            delayTransformQueue.offer(new Runnable() {

                @Override
                public void run() {
                    transformer.transform(reader);
                }
            });

            offset = offset + lengthOf(buffer);
            aggregated = ByteBuffers.aggregate(aggregated, buffer);
        }

        return (ByteBuffer) aggregated.flip();
    }

    protected Iterable<Tuple> toAggregatingQueue() {
        return tupleQueue;
    }

}
