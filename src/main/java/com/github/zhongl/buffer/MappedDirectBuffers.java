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

package com.github.zhongl.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;

import static com.github.zhongl.buffer.DirectByteBufferCleaner.clean;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedDirectBuffers {
    private final Map<DirectBufferMapper, InnerMappedDirectBuffer> cache =
            new HashMap<DirectBufferMapper, InnerMappedDirectBuffer>();

    public MappedDirectBuffer getOrMapBy(DirectBufferMapper directBufferMapper) throws IOException {
        MappedDirectBuffer buffer = cache.get(directBufferMapper);
        return buffer != null ? buffer : mapBy(directBufferMapper);
    }

    public void clear() {
        for (InnerMappedDirectBuffer buffer : buffers()) buffer.release();
    }

    private MappedDirectBuffer mapBy(DirectBufferMapper mapper) throws IOException {
        try {
            tryRelease();
            return tryMapBy(mapper);
        } catch (OutOfMemoryError e) {
            forceRelease();
            return tryMapBy(mapper);
        }
    }

    private InnerMappedDirectBuffer tryMapBy(DirectBufferMapper mapper) throws IOException {
        InnerMappedDirectBuffer buffer = new InnerMappedDirectBuffer(mapper);
        cache.put(mapper, buffer);
        return buffer;
    }

    private void forceRelease() {
        InnerMappedDirectBuffer[] buffers = cache.values().toArray(new InnerMappedDirectBuffer[1]);
        Arrays.sort(buffers);
        buffers[0].release();
    }

    private void tryRelease() {
        for (InnerMappedDirectBuffer buffer : buffers()) buffer.tryRelease();
    }

    List<InnerMappedDirectBuffer> buffers() {
        return new ArrayList<InnerMappedDirectBuffer>(cache.values());
    }

    private class InnerMappedDirectBuffer implements MappedDirectBuffer, Comparable<InnerMappedDirectBuffer> {

        private final DirectBufferMapper mapper;
        private long lastAccessTimeMillis;
        private final MappedByteBuffer buffer;

        private InnerMappedDirectBuffer(DirectBufferMapper mapper) throws IOException {
            this.mapper = mapper;
            buffer = mapper.map();
        }

        @Override
        public <T> int writeBy(Accessor<T> accessor, int offset, T object) {
            lastAccessTimeMillis = System.currentTimeMillis();
            ByteBuffer byteBuffer = buffer.duplicate();
            byteBuffer.position(offset);
            return accessor.write(object, byteBuffer);
        }

        @Override
        public <T> T readBy(Accessor<T> accessor, int offset) {
            lastAccessTimeMillis = System.currentTimeMillis();
            ByteBuffer byteBuffer = buffer.duplicate();
            byteBuffer.position(offset);
            return accessor.read(byteBuffer);
        }

        @Override
        public void release() {
            clean(buffer);
            cache.remove(mapper);
        }

        @Override
        public void flush() {
            lastAccessTimeMillis = System.currentTimeMillis();
            buffer.force();
        }

        @Override
        public int compareTo(InnerMappedDirectBuffer that) {
            return (int) (this.idleTimeMillis() - that.idleTimeMillis());
        }

        void tryRelease() { if (idleTimeMillis() >= mapper.maxIdleTimeMillis()) release(); }

        private long idleTimeMillis() {return System.currentTimeMillis() - lastAccessTimeMillis;}
    }
}
