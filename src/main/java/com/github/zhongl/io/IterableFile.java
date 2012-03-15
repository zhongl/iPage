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

package com.github.zhongl.io;

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class IterableFile {
    protected static final int BUFFER_SIZE = Integer.getInteger("ipage.iterable.file.buffer.size", 1024) * 1024; // 1M

    protected final FileInputStream stream;

    public IterableFile(File file) {
        try {
            stream = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public <T> Iterator<T> toIterator(final Function<ByteBuffer, T> function) {
        return new AbstractIterator<T>() {
            private ByteBuffer byteBuffer = (ByteBuffer) ByteBuffer.allocateDirect(BUFFER_SIZE).position(BUFFER_SIZE);
            private long position = 0;

            @Override
            protected synchronized T computeNext() {
                while (true) {
                    try {
                        int last = byteBuffer.position();
                        T object = function.apply(byteBuffer);
                        position += byteBuffer.position() - last;
                        return object;
                    } catch (BufferUnderflowException e) {
                        try {
                            if (position >= stream.getChannel().size()) {
                                DirectByteBufferCleaner.clean(byteBuffer);
                                Closeables.closeQuietly(stream);
                                return endOfData();
                            }
                            byteBuffer.rewind();
                            stream.getChannel().position(position).read(byteBuffer);
                            byteBuffer.flip();
                        } catch (IOException ex) {
                            throw new IllegalStateException(ex);
                        }
                    }
                }
            }
        };
    }

}
