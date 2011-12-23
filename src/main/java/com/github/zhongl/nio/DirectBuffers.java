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

package com.github.zhongl.nio;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class DirectBuffers {

    private static final int BUFFER_POOL_SIZE = 3;
    private static ThreadLocal[] bufferPool;

    static {
        bufferPool = new ThreadLocal[BUFFER_POOL_SIZE];
        for (int i = 0; i < BUFFER_POOL_SIZE; i++)
            bufferPool[i] = new ThreadLocal();
    }

    private DirectBuffers() {}

    public static ByteBuffer get(int size) {
        ByteBuffer buffer = getExistedBufferCapacityNotLessThan(size);
        return buffer != null ? buffer : ByteBuffer.allocateDirect(size);
    }

    public static void release(ByteBuffer buffer) {
        if (buffer == null) return;
        if (tryHold(buffer)) return;
        tryReplaceSmallerOneBy(buffer);
    }

    private static ByteBuffer getExistedBufferCapacityNotLessThan(int size) {
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            SoftReference ref = ref(i);
            if (ref == null) return null;
            ByteBuffer buffer = (ByteBuffer) ref.get();
            if (buffer != null && buffer.capacity() >= size) {
                buffer.rewind();
                buffer.limit(size);
                bufferPool[i].set(null);
                return buffer;
            }
        }
        return null;
    }

    private static void tryReplaceSmallerOneBy(ByteBuffer buffer) {
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            ByteBuffer inCacheBuf = (ByteBuffer) ref(i).get();
            if ((inCacheBuf == null) || (buffer.capacity() > inCacheBuf.capacity())) {
                bufferPool[i].set(new SoftReference(buffer));
                return;
            }
        }
    }

    private static SoftReference ref(int i) {return (SoftReference) (bufferPool[i].get());}

    private static boolean tryHold(ByteBuffer buffer) {
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            SoftReference ref = ref(i);
            if ((ref == null) || (ref.get() == null)) {
                bufferPool[i].set(new SoftReference(buffer));
                return true;
            }
        }
        return false;
    }

}
