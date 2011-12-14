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

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public final class Builder<T> {

    private static final int UNSET = -1;

    private final File baseDir;
    private int chunkCapacity = UNSET;
    private int minCollectLength = UNSET;
    private Accessor<T> accessor;
    private long maxIdleTimeMillis;

    Builder(File dir) {
        if (!dir.exists()) checkState(dir.mkdirs(), "Can not create directory: %s", dir);
        checkArgument(dir.isDirectory(), "%s should be a directory.", dir);
        baseDir = dir;
    }

    public Builder<T> chunkCapacity(int value) {
        checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
        checkArgument(value >= Chunk.DEFAULT_CAPACITY, "Chunk capacity should not less than %s", Chunk.DEFAULT_CAPACITY);
        chunkCapacity = value;
        return this;
    }

    public Builder<T> maxIdleTimeMillis(long value) {
        checkArgument(value >= 0, "maximize idle time milliseconds should not less than 0");
        maxIdleTimeMillis = value;
        return this;
    }

    public Builder<T> minCollectLength(int value) {
        checkArgument(value >= 0, "minimize collect length should not less than 0");
        minCollectLength = value;
        return this;
    }

    public Builder<T> accessor(Accessor<T> instance) {
        checkNotNull(instance);
        this.accessor = instance;
        return this;
    }

    public IPage<T> build() throws IOException {
        chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
        minCollectLength = (minCollectLength == UNSET) ? 4096 : minCollectLength;
        maxIdleTimeMillis = (maxIdleTimeMillis == UNSET) ? 1000 * 5 : minCollectLength;
        checkNotNull(accessor, "EntryAccessor should not be null.");
        return new IPage<T>(new ChunkList<T>(baseDir, chunkCapacity, accessor, minCollectLength, maxIdleTimeMillis));
    }

}
