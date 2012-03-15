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
import com.google.common.io.Closeables;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class FileAppender {
    private static final int BATCH_KB = Integer.getInteger("ipage.file.appender.batch.kb", 1024) * 1024;// 1M

    private final ByteBuffer batchBuffer;
    private final File file;
    private final FileOutputStream fileOutputStream;

    /** An {@link OutOfMemoryError} should be handled. */
    public FileAppender(File file) {
        this.file = file;
        batchBuffer = ByteBuffer.allocateDirect(BATCH_KB);
        try {
            fileOutputStream = new FileOutputStream(file, false);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Deprecated
    public int append(ByteBuffer buffer) throws IOException {
        int length = buffer.remaining();
        if (length > batchBuffer.remaining()) write();
        batchBuffer.put(buffer);
        return length;
    }

    public int append(Function<ByteBuffer, Void> function) throws IOException {
        while (true) {
            try {
                int position = batchBuffer.position();
                function.apply(batchBuffer);
                return batchBuffer.position() - position;
            } catch (BufferOverflowException e) {
                write();
            }
        }
    }

    public int transferFrom(FileChannel channel, long position, int length) throws IOException {
        checkState(
                channel.transferTo(position, length, thisChannel()) != length,
                "Unexpected transfer length, there may be a bug in FileChannelImpl#transferTo, you should take care of it."
        );
        return length;
    }

    public File force() throws IOException {
        if (!thisChannel().isOpen()) return file;
        try {
            if (batchBuffer.position() > 0) write();
            thisChannel().force(false);
            DirectByteBufferCleaner.clean(batchBuffer);
            return file;
        } finally {
            Closeables.closeQuietly(fileOutputStream);
        }
    }

    private void write() throws IOException {
        batchBuffer.flip();
        thisChannel().write(batchBuffer);
        batchBuffer.rewind();
    }

    private FileChannel thisChannel() {return fileOutputStream.getChannel();}
}
