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
import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.nio.Forcer;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class DefaultBatch extends Batch {
    protected final Function<Tuple> function;
    protected final Queue<Runnable> transformingQueue;

    private final Queue<Tuple> tupleQueue;

    public DefaultBatch(final File file, int position, final Codec codec, int estimateBufferSize) {
        super(file, position, codec, estimateBufferSize);
        this.transformingQueue = new LinkedList<Runnable>();

        this.function = new Function<Tuple>() {
            @Override
            public int apply(final Tuple tuple, final int offset) throws IOException {
                transformingQueue.offer(new Runnable() {

                    @Override
                    public void run() {
                        tuple.transformer.transform(new Reader<Object>(file, offset, codec));
                    }
                });

                ByteBuffer buffer = tuple.buffer;
                int length = ByteBuffers.lengthOf(buffer);
                aggregate(buffer);
                return length;
            }
        };

        this.tupleQueue = new LinkedList<Tuple>();
    }

    protected void onAppend(final ObjectRef<?> objectRef, final Transformer<?> transformer) {
        tupleQueue.offer(new Tuple(transformer, objectRef.encode()));
    }

    protected void aggregate() throws IOException {
        poll(tupleQueue, function);
    }

    protected final <T> void poll(Queue<T> queue, Function<T> function) throws IOException {
        int offset = position;
        for (T e : queue) offset += function.apply(e, offset);
    }

    protected final void aggregate(ByteBuffer buffer) {
        if (aggregatedBuffer == null)
            aggregatedBuffer = ByteBuffer.allocate(this.estimateBufferSize);

        while (aggregatedBuffer.remaining() < ByteBuffers.lengthOf(buffer)) {
            aggregatedBuffer.flip();
            aggregatedBuffer = ByteBuffer.allocate(estimateBufferSize *= 2)
                                         .put(aggregatedBuffer);
        }
        aggregatedBuffer.put(buffer);
    }

    @Override
    protected <T> Cursor<T> _append(T object) {
        final ObjectRef<T> objectRef = new ObjectRef<T>(object, codec);
        final Transformer<T> transformer = new Transformer<T>(objectRef);
        onAppend(objectRef, transformer);
        return transformer;
    }

    @Override
    protected void _writeAndForceTo(FileChannel channel) throws IOException {
        aggregate();
        aggregatedBuffer.flip();

        Forcer.getInstance().force(channel, aggregatedBuffer);

        while (!transformingQueue.isEmpty())
            transformingQueue.poll().run();
    }

    interface Function<T> {
        int apply(T object, int offset) throws IOException;
    }

    protected class Tuple {
        public final Transformer<?> transformer;
        public final ByteBuffer buffer;

        public Tuple(Transformer<?> transformer, ByteBuffer buffer) {
            this.transformer = transformer;
            this.buffer = buffer;
        }
    }
}
