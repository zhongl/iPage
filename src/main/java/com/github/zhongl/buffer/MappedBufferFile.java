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

package com.github.zhongl.buffer;

import com.google.common.io.Files;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link MappedBufferFile}
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
public class MappedBufferFile implements Comparable {
    final static List<MappedBufferFile> aliveFiles = new ArrayList<MappedBufferFile>();

    public static MappedBufferFile writeable(File file, int capacity) {
        return newMappedBufferFile(new InnerDirectBufferMapper(file, capacity, false), Long.MAX_VALUE);
    }

    public static MappedBufferFile readOnly(File file, long maxIdleTimeMIllis) {
        return newMappedBufferFile(new InnerDirectBufferMapper(file, (int) file.length(), true), maxIdleTimeMIllis);
    }

    private static MappedBufferFile newMappedBufferFile(DirectBufferMapper mapper, long maxIdleTimeMIllis) {
        MappedBufferFile mappedBufferFile = new MappedBufferFile(mapper, maxIdleTimeMIllis);
        aliveFiles.add(mappedBufferFile);
        return mappedBufferFile;
    }

    private static void forceRelease() {
        Collections.sort(aliveFiles);
        aliveFiles.get(0).release0();
    }

    private static void tryRelease() {
        for (MappedBufferFile mappedBufferFile : aliveFiles) {
            if (mappedBufferFile.isIdle()) mappedBufferFile.release0();
        }
    }

    private final DirectBufferMapper mapper;
    private final long maxIdleTimeMillis;

    private MappedByteBuffer buffer;
    private long lastAccessTimeMillis;

    MappedBufferFile(DirectBufferMapper mapper, long maxIdleTimeMillis) {
        this.mapper = mapper;
        this.maxIdleTimeMillis = maxIdleTimeMillis;
    }

    public static ByteBuffer slice(ByteBuffer buffer, int offset, int length) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(offset);
        duplicate.limit(offset + length);
        return duplicate.slice();
    }

    public <T> int writeBy(Accessor<T> accessor, int offset, T object)
            throws ReadOnlyBufferException, BufferOverflowException, IOException {
        int length = accessor.byteLengthOf(object);
        if (offset + length > buffer().limit()) throw new BufferOverflowException();
        return accessor.write(object, slice(buffer(), offset, length));
    }

    public void flush() {
        if (isReleased()) return;
        buffer.force();
    }

    public <T> T readBy(Accessor<T> accessor, int offset, int length) throws IOException {
        return accessor.read(slice(buffer(), offset, length));
    }

    public void release() {
        release0();
        aliveFiles.remove(this);
    }

    private void release0() {
        if (isReleased()) return;
        DirectByteBufferCleaner.clean(buffer);
        buffer = null;
    }

    boolean isReleased() {return buffer == null;}

    private boolean isIdle() { return remainIdleTimeMillis() >= 0; }

    private long remainIdleTimeMillis() {
        return System.currentTimeMillis() - lastAccessTimeMillis - maxIdleTimeMillis;
    }

    private ByteBuffer buffer() throws IOException {
        lastAccessTimeMillis = System.currentTimeMillis();
        if (buffer == null) {
            try {
                tryRelease();
                buffer = mapper.map();
            } catch (OutOfMemoryError e) {
                forceRelease();
                buffer = mapper.map();
            }
        }
        return buffer;
    }

    @Override
    public int compareTo(Object o) {
        MappedBufferFile that = (MappedBufferFile) o;
        return (int) (this.remainIdleTimeMillis() - that.remainIdleTimeMillis());
    }

    private static class InnerDirectBufferMapper implements DirectBufferMapper {
        private final File file;
        private final int capacity;
        private final boolean readOnly;

        public InnerDirectBufferMapper(File file, int capacity, boolean readOnly) {
            this.file = file;
            this.capacity = capacity;
            this.readOnly = readOnly;
        }

        @Override
        public MappedByteBuffer map() throws IOException {
            return Files.map(file, readOnly ? READ_ONLY : READ_WRITE, capacity);
        }

        @Override
        public long maxIdleTimeMillis() {
            return 7000L;
        }
    }
}
