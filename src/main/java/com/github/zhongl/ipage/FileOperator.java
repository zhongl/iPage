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

package com.github.zhongl.ipage;

import com.github.zhongl.buffer.DirectBufferMapper;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link com.github.zhongl.ipage.FileOperator}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
class FileOperator implements DirectBufferMapper {
    private final File file;
    private final long capacity;
    private final boolean readOnly;
    private final long maxIdleTimeMillis;
    private final long beginPosition;

    public static FileOperator readOnly(File file, long maxIdleTimeMillis) throws IOException {
        return new FileOperator(file, file.length(), true, maxIdleTimeMillis);
    }

    public static FileOperator writeable(File file, int capacity) throws IOException {
        return new FileOperator(file, capacity, false, Long.MAX_VALUE);
    }

    FileOperator(File file, long capacity, boolean readOnly, long maxIdleTimeMillis) throws IOException {
        this.file = file;
        this.capacity = capacity;
        this.readOnly = readOnly;
        this.maxIdleTimeMillis = maxIdleTimeMillis;
        beginPosition = Long.parseLong(this.file.getName());
    }

    public long beginPosition() { return beginPosition; }

    public void delete() { checkState(file.delete(), "Can't delete file %s", file); }

    /**
     * @return {@code this} after truncated file.
     * @see Chunk#left(long)
     */
    public FileOperator left(long offset) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(offset);
        randomAccessFile.close();
        return this;
    }

    /**
     * @return a new instance of {@link com.github.zhongl.ipage.FileOperator} with new file.
     * @see Chunk#right(long)
     */
    public FileOperator right(long offset) throws IOException {
        File newFile = new File(file.getParentFile(), Long.toString(offset));
        long position = offset - beginPosition();
        long length = file.length() - position;
        InputSupplier<InputStream> from = ByteStreams.slice(Files.newInputStreamSupplier(file), position, length);
        Files.copy(from, newFile);
        return readOnly(newFile, maxIdleTimeMillis);
    }

    @Override
    public MappedByteBuffer map() throws IOException {
        return readOnly ? Files.map(file) : Files.map(file, READ_WRITE, capacity);
    }

    @Override
    public long maxIdleTimeMillis() { return maxIdleTimeMillis; }

    public long length() { return file.length(); }

    public FileOperator asReadOnly() throws IOException {
        return readOnly ? this : readOnly(file, maxIdleTimeMillis);
    }
}
