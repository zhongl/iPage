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

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class MappedFile {
    protected final File file;
    protected final long capacity;
    protected volatile MappedByteBuffer mappedByteBuffer;

    public MappedFile(File file, long capacity) {
        this.file = file;
        this.capacity = capacity;
    }

    public boolean flush() throws IOException {
        if (mappedByteBuffer == null) return false;
        mappedByteBuffer.force();
        return true;
    }

    protected final boolean releaseBuffer() throws IOException {
        if (mappedByteBuffer == null) return false;
        DirectByteBufferCleaner.clean(mappedByteBuffer);
        mappedByteBuffer = null;
        return true;
    }

    protected final void ensureMap() throws IOException {
        // TODO maybe a closed state is needed for prevent remap
        if (mappedByteBuffer == null) mappedByteBuffer = Files.map(file, READ_WRITE, capacity);
    }

    protected final void deleteFile() throws IOException {
        setLength(0L);
        File deleted = new File(file.getParentFile(), "-" + file.getName() + "-");
        checkState(file.renameTo(deleted), "Can't delete file %s", file); // void deleted failed
        checkState(deleted.delete(), "Can't delete file %s", file);
    }

    protected final void setLength(long value) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(value);
        randomAccessFile.close();
    }
}
