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

import com.github.zhongl.buffer.Accessor;
import com.github.zhongl.buffer.MappedDirectBuffers;
import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

class ChunkFactory<T> {
    private final File baseDir;
    private final Accessor<T> accessor;
    private final int minimizeChunkCapacity;
    private final MappedDirectBuffers buffers;
    private final long maxIdleTimeMillis;

    public ChunkFactory(File baseDir, Accessor<T> accessor, int minimizeChunkCapacity, long maxIdleTimeMillis) {
        this.baseDir = baseDir;
        this.accessor = accessor;
        this.minimizeChunkCapacity = minimizeChunkCapacity;
        this.maxIdleTimeMillis = maxIdleTimeMillis;
        this.buffers = new MappedDirectBuffers();
    }

    public Chunk<T> appendableChunkOn(File file) throws IOException {
        return new AppendableChunk<T>(buffers, FileOperator.writeable(file, minimizeChunkCapacity), accessor);
    }

    public Chunk<T> readOnlyChunkOn(File file) throws IOException {
        return new ReadOnlyChunk<T>(buffers, FileOperator.readOnly(file, maxIdleTimeMillis), accessor);
    }

    public Chunk<T> newAppendableAfter(Chunk<T> last) throws IOException {
        return appendableChunkOn(new File(baseDir, Long.toString(last.endPosition() + 1)));
    }

    public Chunk<T> newFirstAppendableChunk() throws IOException {
        try {
            return appendableChunkOn(new File(baseDir, "0"));
        } catch (FileNotFoundException e) {
            Preconditions.checkState(baseDir.mkdirs(), "Can't mkdirs %s", baseDir);
            return newFirstAppendableChunk();
        }
    }
}