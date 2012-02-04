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

package com.github.zhongl.ex.nio;

import com.google.common.cache.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyMappedBuffers {

    public static final long DURATION = Long.getLong("ipage.buffer.cache.duration", 1000L);

    private static final Cache<File, MappedByteBuffer> CACHE;

    static {
        CACHE = CacheBuilder.newBuilder()
                            .expireAfterAccess(DURATION, TimeUnit.MILLISECONDS)
                            .removalListener(new RemovalListener<File, MappedByteBuffer>() {
                                @Override
                                public void onRemoval(RemovalNotification<File, MappedByteBuffer> notification) {
                                    DirectByteBufferCleaner.clean(notification.getValue());
                                }
                            })
                            .build(new CacheLoader<File, MappedByteBuffer>() {
                                @Override
                                public MappedByteBuffer load(File key) throws Exception {
                                    return FileChannels.getOrOpen(key).map(READ_ONLY, 0L, key.length());
                                }
                            });
    }


    public static ByteBuffer getOrMap(File file) {
        checkNotNull(file);
        checkState(file.exists());

        MappedByteBuffer buffer = CACHE.getUnchecked(file);
        if (ByteBuffers.lengthOf(buffer) == file.length()) { // mapped may not sync with file content.
            if (!buffer.isLoaded()) buffer.load();
            return buffer.duplicate();
        }

        clearMappedOf(file); // clear for reload.
        return CACHE.getUnchecked(file).duplicate();
    }

    public static void clearMappedOf(File file) {
        CACHE.invalidate(file);
        CACHE.cleanUp();
    }

    public static void clearAll() {
        CACHE.invalidateAll();
        CACHE.cleanUp();
    }

    public static CacheStats stats() {
        return CACHE.stats();
    }
}
