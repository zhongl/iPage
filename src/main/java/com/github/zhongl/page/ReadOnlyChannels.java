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

package com.github.zhongl.page;

import com.google.common.cache.*;
import com.google.common.io.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ReadOnlyChannels {

    private final Cache<File, FileChannel> readableByteChannelCache;

    public ReadOnlyChannels() {
        readableByteChannelCache = CacheBuilder.newBuilder()
                                               .expireAfterAccess(1L, TimeUnit.SECONDS)
                                               .concurrencyLevel(1)
                                               .maximumSize(4096)
                                               .removalListener(new RemovalListener<File, FileChannel>() {
                                                   @Override
                                                   public void onRemoval(RemovalNotification<File, FileChannel> notification) {
                                                       Closeables.closeQuietly(notification.getValue());
                                                   }
                                               })
                                               .build(new CacheLoader<File, FileChannel>() {
                                                   @Override
                                                   public FileChannel load(File key) throws Exception {
                                                       return new FileInputStream(key).getChannel();
                                                   }
                                               });
    }

    public FileChannel getOrCreateBy(File file) {
        return readableByteChannelCache.getUnchecked(file);
    }

    public void close(File file) {
        readableByteChannelCache.invalidate(file);
    }
}
