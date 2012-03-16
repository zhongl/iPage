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
import com.google.common.io.Closeables;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileChannels {

    private FileChannels() {}

    public static <T> T read(File file, FileChannelFunction<T> function) throws IOException {
        return read(new FileInputStream(file), function);
    }

    public static <T> T read(File file, final long position, final int length, final Function<ByteBuffer, T> function) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(length);
        return read(new FileInputStream(file), new FileChannelFunction<T>() {
            @Override
            public T apply(FileChannel channel) throws IOException {
                read(channel, position, byteBuffer);
                return function.apply(byteBuffer);
            }
        });
    }

    public static void read(FileChannel channel, long position, ByteBuffer byteBuffer) throws IOException {
        channel.position(position);
        boolean endOfChannel = false;
        while (!endOfChannel && byteBuffer.hasRemaining())
            endOfChannel = (channel.read(byteBuffer) == -1);
        byteBuffer.flip();
    }

    private static <T> T read(FileInputStream stream, FileChannelFunction<T> function) throws IOException {
        try {
            return read(stream.getChannel(), function);
        } finally {
            Closeables.closeQuietly(stream);
        }
    }

    private static <T> T read(FileChannel channel, FileChannelFunction<T> function) throws IOException {
        try {
            return function.apply(channel);
        } finally {
            Closeables.closeQuietly(channel);
        }
    }

    public static interface FileChannelFunction<T> {
        public T apply(FileChannel channel) throws IOException;
    }

}
