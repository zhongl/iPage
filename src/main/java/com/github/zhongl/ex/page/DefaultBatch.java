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
import com.github.zhongl.ex.util.Tuple;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class DefaultBatch<V> extends AbstractBatch<V> {

    protected final Codec codec;

    protected long position;

    public DefaultBatch(Codec codec, long position, int estimateBufferSize) {
        super(estimateBufferSize);
        this.codec = codec;
        this.position = position;
    }

    @Override
    protected Tuple tuple(FutureCallback<Cursor> callback, V value) {
        return new Tuple(callback, codec.encode(value));
    }

    @Override
    protected Tuple aggregate(Tuple tuple, ByteBuffer aggregated) {
        ByteBuffer buffer = bufferIn(tuple);

        long offset = position;
        int length = ByteBuffers.lengthOf(buffer);
        ByteBuffers.aggregate(aggregated, buffer);
        position += length;

        return new Tuple(callbackIn(tuple), offset, length);
    }

    protected ByteBuffer bufferIn(Tuple tuple) {
        return tuple.get(1);
    }

    protected FutureCallback<Cursor> callbackIn(Tuple tuple) {
        return tuple.get(0);
    }

    @Override
    protected void callback(Tuple tuple, Throwable error) {
        FutureCallback<Cursor> callback = callbackIn(tuple);
        if (error != null) callback.onFailure(error);
        else callback.onSuccess(cursor(tuple.<Long>get(1), tuple.<Integer>get(2)));
    }

    protected Cursor cursor(long offset, int length) {return new DefaultCursor(offset, length);}

}
