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

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Stores {
    private final Map<FileChannelFactory, InnerStore> cache =
            new HashMap<FileChannelFactory, InnerStore>();

    public Store getOrCreateBy(FileChannelFactory fileChannelFactory) throws IOException {
        Store buffer = cache.get(fileChannelFactory);
        return buffer != null ? buffer : createBy(fileChannelFactory);
    }

    public void clear() throws IOException {
        for (InnerStore buffer : stores()) buffer.release();
    }

    private Store createBy(FileChannelFactory factory) throws IOException {
        try {
            tryRelease();
            return tryCreateBy(factory);
        } catch (OutOfMemoryError e) {
            forceRelease();
            return tryCreateBy(factory);
        }
    }

    private InnerStore tryCreateBy(FileChannelFactory factory) throws IOException {
        InnerStore buffer = new InnerStore(factory);
        cache.put(factory, buffer);
        return buffer;
    }

    private void forceRelease() throws IOException {
        InnerStore[] buffers = cache.values().toArray(new InnerStore[1]);
        Arrays.sort(buffers);
        buffers[0].release();
    }

    private void tryRelease() throws IOException {
        for (InnerStore store : stores()) store.tryRelease();
    }

    List<InnerStore> stores() {
        return new ArrayList<InnerStore>(cache.values());
    }

    private class InnerStore implements Store, Comparable<InnerStore> {

        private static final int DEFAULT_BUFFER_SIZE = 4096;
        private final FileChannelFactory factory;
        private long lastAccessTimeMillis;
        private final FileChannel channel;


        private InnerStore(FileChannelFactory factory) throws IOException {
            this.factory = factory;
            channel = this.factory.create();
        }

        @Override
        public <T> int writeBy(Accessor<T> accessor, int offset, T object) throws IOException {
            lastAccessTimeMillis = System.currentTimeMillis();

            ByteBuffer byteBuffer = DirectBuffers.get(accessor.byteLengthOf(object));
            try {
                accessor.write(object, byteBuffer);
                byteBuffer.flip();
                channel.position(offset);
                return channel.write(byteBuffer);
            } finally {
                DirectBuffers.release(byteBuffer);
            }
        }

        @Override
        public <T> T readBy(Accessor<T> accessor, int offset) throws IOException {
            lastAccessTimeMillis = System.currentTimeMillis();
            ByteBuffer byteBuffer = null;
            for (int size = DEFAULT_BUFFER_SIZE; ; size = size * 2) {
                try {
                    byteBuffer = DirectBuffers.get(size);
                    byte b = byteBuffer.get(0);
                    channel.position(offset);
                    channel.read(byteBuffer);
                    byteBuffer.flip();
                    return accessor.read(byteBuffer);
                } catch (BufferUnderflowException e) { // retry
                } finally {
                    if (byteBuffer != null) DirectBuffers.release(byteBuffer);
                }
            }
        }

        @Override
        public void release() throws IOException {
            channel.close();
            cache.remove(factory);
        }

        @Override
        public void flush() throws IOException {
            lastAccessTimeMillis = System.currentTimeMillis();
            channel.force(true);
        }

        @Override
        public int compareTo(InnerStore that) {
            return (int) (this.idleTimeMillis() - that.idleTimeMillis());
        }

        void tryRelease() throws IOException { if (idleTimeMillis() >= factory.maxIdleTimeMillis()) release(); }

        private long idleTimeMillis() {return System.currentTimeMillis() - lastAccessTimeMillis;}

    }
}
