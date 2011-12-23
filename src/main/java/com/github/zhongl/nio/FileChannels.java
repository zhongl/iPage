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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileChannels {
    private FileChannels() {}

    public static FileChannel channel(File file, int length) throws IOException {
        return new InnerFileChannel(file, length);
    }

    public static FileChannel channel(File file) throws IOException {
        return new InnerFileChannel(file);
    }

    private static class InnerFileChannel extends FileChannel {

        private final RandomAccessFile randomAccessFile;

        public InnerFileChannel(File file, int length) throws IOException {
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(length);
        }

        public InnerFileChannel(File file) throws FileNotFoundException {
            randomAccessFile = new RandomAccessFile(file, "r");
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {return channel().read(dst);}

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return channel().read(dsts, offset, length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            checkOverflow(channel().position(), src.limit());
            return channel().write(src);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            checkOverflow(channel().position(), length);
            return channel().write(srcs, offset, length);
        }

        @Override
        public long position() throws IOException {return channel().position();}

        @Override
        public FileChannel position(long newPosition) throws IOException {
            return channel().position(newPosition);
        }

        @Override
        public long size() throws IOException {return channel().size();}

        @Override
        public FileChannel truncate(long size) throws IOException {return channel().truncate(size);}

        @Override
        public void force(boolean metaData) throws IOException {channel().force(metaData);}

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            return channel().transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            return channel().transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return channel().read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            checkOverflow(position, src.limit());
            return channel().write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return channel().map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return channel().lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return channel().tryLock(position, size, shared);
        }

        @Override
        protected void implCloseChannel() throws IOException {
            randomAccessFile.close();
        }

        private FileChannel channel() {return randomAccessFile.getChannel();}

        private void checkOverflow(long position, int limit) throws IOException {
            if (position + limit > randomAccessFile.length()) throw new BufferOverflowException();
        }
    }
}
