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
import com.google.common.io.Closeables;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileChannels {

    public static final long DURATION = Long.getLong("ipage.channel.cache.duration", 1000L);

    private static final Cache<File, FileChannel> CACHE;

    static {
        CACHE = CacheBuilder.newBuilder()
                            .expireAfterAccess(DURATION, TimeUnit.MILLISECONDS)
                            .removalListener(new RemovalListener<File, FileChannel>() {
                                @Override
                                public void onRemoval(RemovalNotification<File, FileChannel> notification) {
                                    Closeables.closeQuietly(notification.getValue());
                                }
                            })
                            .build(new CacheLoader<File, FileChannel>() {
                                @Override
                                public FileChannel load(File key) throws Exception {
                                    return new RAFileChannel(key);
                                }
                            });
    }


    public static FileChannel getOrOpen(File file) {
        return CACHE.getUnchecked(file);
    }

    public static void closeChannelOf(File file) {
        CACHE.invalidate(file);
    }

    public static void closeAll() {
        CACHE.invalidateAll();
        CACHE.cleanUp();
    }

}
