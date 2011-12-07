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

import com.github.zhongl.accessor.Accessor;
import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public final class IPageBuilder<T> {

    private static final int UNSET = -1;

    private final File baseDir;
    private int chunkCapacity = UNSET;
    private Accessor<T> accessor;

    IPageBuilder(File dir) {
        if (!dir.exists()) checkState(dir.mkdirs(), "Can not create directory: %s", dir);
        checkArgument(dir.isDirectory(), "%s should be a directory.", dir);
        baseDir = dir;
    }

    public IPageBuilder<T> chunkCapacity(int value) {
        checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
        checkArgument(value >= Chunk.DEFAULT_CAPACITY, "Chunk capacity should not less than %s", Chunk.DEFAULT_CAPACITY);
        chunkCapacity = value;
        return this;
    }

    public IPageBuilder<T> byteBufferAccessor(Accessor<T> instance) {
        checkNotNull(instance);
        this.accessor = instance;
        return this;
    }

    public IPage<T> build() throws IOException {
        chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
        checkNotNull(accessor, "Accessor should not be null.");
        ChunkFactory<T> chunkFactory = new ChunkFactory<T>(chunkCapacity, accessor);
        List<Chunk<T>> chunks = loadExistChunks(chunkFactory);
        return new IPage<T>(baseDir, chunkFactory, chunks);
    }

    private List<Chunk<T>> loadExistChunks(ChunkFactory<T> chunkFactory) throws IOException {
        File[] files = baseDir.listFiles(new NumberFileNameFilter());
        Arrays.sort(files, new FileNumberNameComparator());

        ArrayList<Chunk<T>> chunks = new ArrayList<Chunk<T>>(files.length);
        for (File file : files) {
            long beginPositionInIPage = Long.parseLong(file.getName());
            chunks.add(0, chunkFactory.create(beginPositionInIPage, file)); // reverse order to make sure the appending chunk at first.
        }
        return chunks;
    }

}
