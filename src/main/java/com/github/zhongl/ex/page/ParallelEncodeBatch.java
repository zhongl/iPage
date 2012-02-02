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

import com.github.zhongl.ex.lang.Tuple;
import com.google.common.collect.AbstractIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ParallelEncodeBatch extends DefaultBatch {

    private final static ExecutorService SERVICE;

    static {
        SERVICE = Executors.newFixedThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    private final Queue<Future<Tuple>> futureQueue;

    public ParallelEncodeBatch(CursorFactory cursorFactory, int position, int estimateBufferSize) {
        super(cursorFactory, position, estimateBufferSize);
        this.futureQueue = new LinkedList<Future<Tuple>>();
    }

    @Override
    protected void onAppend(final ObjectRef objectRef, final Proxy proxy) {
        futureQueue.offer(SERVICE.submit(new Callable<Tuple>() {
            @Override
            public Tuple call() throws Exception {
                return new Tuple(proxy, objectRef.encode());
            }

        }));
    }

    @Override
    protected Iterable<Tuple> toAggregatingQueue() {
        return new Iterable<Tuple>() {
            @Override
            public Iterator<Tuple> iterator() {
                return new AbstractIterator<Tuple>() {
                    @Override
                    protected Tuple computeNext() {
                        try {
                            Future<Tuple> future = futureQueue.poll();
                            return future == null ? endOfData() : future.get();
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e.getCause());
                        }

                    }
                };
            }
        };
    }
}
