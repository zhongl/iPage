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
import com.google.common.io.Files;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.PRIVATE;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class DirectBuffer {

    private final ReentrantReadWriteLock lock;

    @GuardedBy("lock")
    private ByteBuffer byteBuffer;

    @GuardedBy("lock")
    private File file;

    public DirectBuffer() { lock = new ReentrantReadWriteLock(); }

    public <T> T read(Function<ByteBuffer, T> function) {
        checkState(byteBuffer != null, "DirectBuffer have not loaded.");
        lock.readLock().lock();
        try {
            return function.apply(byteBuffer.asReadOnlyBuffer());
        } finally {
            lock.readLock().unlock();
        }
    }

    public DirectBuffer loadFrom(File file) throws IOException {
        if (file == null || !file.exists()) {
            byteBuffer = ByteBuffer.allocate(0);
            return this;
        }

        if (file.length() > Integer.MAX_VALUE)
            throw new OutOfMemoryError("Direct buffer can't load from a file length greater than 2G.");

        lock.writeLock().lock();
        try {
            return mapFrom(file);
        } catch (OutOfMemoryError e) {
            if (file.length() > byteBuffer.capacity())
                throw new OutOfMemoryError("Try loaded but failed.");

            return readFrom(file);
        } finally {
            this.file = file;
            lock.writeLock().unlock();
        }
    }

    public File backendFile() {
        lock.readLock().lock();
        try {
            return file;
        } finally {
            lock.readLock().unlock();
        }
    }

    private DirectBuffer mapFrom(File file) throws IOException {
        ByteBuffer toClean = byteBuffer;
        byteBuffer = Files.map(file, PRIVATE);
        DirectByteBufferCleaner.clean(toClean);
        return this;
    }

    private DirectBuffer readFrom(File file) throws IOException {
        FileInputStream stream = new FileInputStream(file);

        try {
            byteBuffer.rewind();
            stream.getChannel().read(byteBuffer);
            byteBuffer.flip();

            return this;
        } finally {
            Closeables.closeQuietly(stream);
        }
    }
}
