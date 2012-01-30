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
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ParallelEncodeBatch extends DefaultBatch {

    private final static ExecutorService SERVICE;

    static {
        SERVICE = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    private final Queue<Future<Tuple>> futureQueue;

    public ParallelEncodeBatch(int position, File file, Codec codec, int estimateBufferSize) {
        super(file, position, codec, estimateBufferSize);
        this.futureQueue = new LinkedList<Future<Tuple>>();
    }

    @Override
    protected void onAppend(final ObjectRef<?> objectRef, final Transformer<?> transformer) {
        futureQueue.offer(SERVICE.submit(new Callable<Tuple>() {
            @Override
            public Tuple call() throws Exception {
                return new Tuple(transformer, objectRef.encode());
            }

        }));
    }

    @Override
    protected void aggregate() throws IOException {
        poll(futureQueue, new Function<Future<Tuple>>() {
            @Override
            public int apply(Future<Tuple> future, int offset) throws IOException {
                try {
                    final Tuple tuple = future.get();
                    return function.apply(tuple, offset);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                } catch (ExecutionException e) {
                    Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
                    throw new RuntimeException(e.getCause());
                }
            }
        });
    }

}
