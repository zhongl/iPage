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
import com.github.zhongl.ex.util.Tuple;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class ParallelEncodeBatch<V> extends DefaultBatch<V> {

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

    public ParallelEncodeBatch(Codec codec, int position, int estimateBufferSize) {
        super(codec, position, estimateBufferSize);
    }

    @Override
    protected Tuple tuple(FutureCallback<Cursor> callback, final V value) {
        return new Tuple(callback, SERVICE.submit(new Callable<ByteBuffer>() {
            @Override
            public ByteBuffer call() throws Exception {
                return codec.encode(value);
            }
        }));
    }

    @Override
    protected ByteBuffer bufferIn(Tuple tuple) {
        try {
            return tuple.<Future<ByteBuffer>>get(1).get();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }
}
