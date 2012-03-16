/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.io;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileChannels {

    private static final Cache<ThreadBound, FileInputStream> CACHE;
    public static final Callable<FileInputStream> VALUE_LOADER = new Callable<FileInputStream>() {
        @Override
        public FileInputStream call() throws Exception {
            throw new UnsupportedOperationException();
        }
    };

    static {
        CACHE = CacheBuilder.newBuilder()
                            .concurrencyLevel(Runtime.getRuntime().availableProcessors())
                            .expireAfterAccess(1, TimeUnit.SECONDS)
                            .removalListener(new RemovalListener<ThreadBound, FileInputStream>() {
                                @Override
                                public void onRemoval(RemovalNotification<ThreadBound, FileInputStream> notification) {
                                    Closeables.closeQuietly(notification.getValue());
                                }
                            })
                            .build();
    }

    private FileChannels() {}

    public static <T> T read(final File file, FileChannelFunction<T> function) throws IOException {
        try {
            return function.apply(CACHE.get(new ThreadBound(file), new FileInputStreamLoader(file)).getChannel());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.propagateIfInstanceOf(cause, IOException.class);
            throw new IllegalStateException(cause);
        }
    }

    public static <T> T read(File file, final long position, final int length, final Function<ByteBuffer, T> function) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        try {
            return new FileChannelFunction<T>() {
                @Override
                public T apply(FileChannel channel) throws IOException {
                    read(channel, position, byteBuffer);
                    return function.apply(byteBuffer);
                }
            }.apply(CACHE.get(new ThreadBound(file), new FileInputStreamLoader(file)).getChannel());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Throwables.propagateIfInstanceOf(cause, IOException.class);
            throw new IllegalStateException(cause);
        }
    }

    public static void read(FileChannel channel, long position, ByteBuffer byteBuffer) throws IOException {
        channel.position(position);
        boolean endOfChannel = false;
        while (!endOfChannel && byteBuffer.hasRemaining())
            endOfChannel = (channel.read(byteBuffer) == -1);
        byteBuffer.flip();
    }

    public static interface FileChannelFunction<T> {
        public T apply(FileChannel channel) throws IOException;
    }

    private static class ThreadBound {
        private final File file;
        private final Thread current;

        private ThreadBound(File file) {
            this.file = checkNotNull(file);
            current = Thread.currentThread();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ThreadBound that = (ThreadBound) o;
            return current.equals(that.current) && file.equals(that.file);
        }

        @Override
        public int hashCode() {
            int result = file.hashCode();
            result = 31 * result + current.hashCode();
            return result;
        }
    }

    private static class FileInputStreamLoader implements Callable<FileInputStream> {
        private final File file;

        public FileInputStreamLoader(File file) {this.file = file;}

        @Override
        public FileInputStream call() throws Exception {
            return new FileInputStream(file);
        }
    }
}
