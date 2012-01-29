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

package com.github.zhongl.ex.journal;

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.cache.*;
import com.google.common.io.Closeables;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MappedByteBuffers {

    private static final int MAX_SIZE = Integer.getInteger("ipage.mapped.cache.max.size", 4096);

    private static final long DURATION_MILLISECONDS = Long.getLong("ipage.mapped.cache.duration.milliseconds", 1000L);

    private static final Cache<MappingOptions, MappedByteBuffer> CACHE;

    static {
        CACHE = CacheBuilder.newBuilder()
                            .expireAfterAccess(DURATION_MILLISECONDS, TimeUnit.MILLISECONDS)
                            .maximumSize(MAX_SIZE)
                            .removalListener(new RemovalListener<MappingOptions, MappedByteBuffer>() {
                                @Override
                                public void onRemoval(RemovalNotification<MappingOptions, MappedByteBuffer> notification) {
                                    DirectByteBufferCleaner.clean(notification.getValue());
                                }
                            })
                            .build(new CacheLoader<MappingOptions, MappedByteBuffer>() {
                                @Override
                                public MappedByteBuffer load(MappingOptions mappingOptions) throws Exception {
                                    return map(mappingOptions);
                                }
                            });
    }

    private MappedByteBuffers() { }

    public static CacheStats stats() {
        return CACHE.stats();
    }

    public static MappedByteBuffer getOrMapBy(MappingOptions mappingOptions) {
        MappedByteBuffer buffer = CACHE.getUnchecked(mappingOptions);
        if (!buffer.isLoaded()) buffer.load();
        return buffer;
    }

    public static void release(Object key) {
        CACHE.invalidate(key);
    }

    public static void clear() {
        CACHE.invalidateAll();
        CACHE.cleanUp();
    }

    private static MappedByteBuffer map(MappingOptions mappingOptions) throws IOException {
        String rw = mappingOptions.mode() == FileChannel.MapMode.READ_ONLY ? "r" : "rw";
        RandomAccessFile file = new RandomAccessFile(mappingOptions.file(), rw);
        try {
            return map(file, mappingOptions);
        } finally {
            Closeables.closeQuietly(file);
        }
    }

    private static MappedByteBuffer map(RandomAccessFile file, MappingOptions mappingOptions) throws IOException {
        FileChannel channel = file.getChannel();
        try {
            return channel.map(mappingOptions.mode(), mappingOptions.position(), mappingOptions.size());
        } finally {
            Closeables.closeQuietly(channel);
        }
    }
}
