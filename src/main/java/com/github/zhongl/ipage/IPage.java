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
import com.github.zhongl.builder.*;
import com.github.zhongl.integrity.ValidateOrRecover;
import com.github.zhongl.integrity.Validator;
import com.github.zhongl.util.FileHandler;
import com.github.zhongl.util.NumberNamedFilesLoader;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.ArrayList;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage<T> implements Closeable, ValidateOrRecover<T, IOException> {

    private final ChunkList<T> chunkList;
    private final ChunkFactory<T> chunkFactory;
    private final GarbageCollector<T> garbageCollector;

    public static <T> Builder<T> baseOn(File dir) {
        Builder<T> builder = Builders.newInstanceOf(Builder.class);
        builder.dir(dir);
        return builder;
    }

    IPage(File baseDir,
          Accessor<T> accessor,
          int maximizeChunkCapacity,
          long minimizeCollectLength,
          long maxChunkIdleTimeMillis) throws IOException {
        this.chunkFactory = new ChunkFactory<T>(baseDir, accessor, maximizeChunkCapacity, maxChunkIdleTimeMillis);
        this.chunkList = new ChunkList<T>(loadExistChunksBy(baseDir, chunkFactory));
        this.garbageCollector = new GarbageCollector<T>(chunkList, minimizeCollectLength);
    }

    public long append(T record) throws IOException {
        try {
            if (!chunkList.isEmpty()) return chunkList.last().append(record);
        } catch (BufferOverflowException noSpaceForAppending) {} // chunk no space for appending
        grow();
        return append(record);
    }

    public T get(long offset) throws IOException {
        try {
            return chunkList.chunkIn(offset).get(offset);
        } catch (ArrayIndexOutOfBoundsException canNotFindChunk) {
        } catch (IndexOutOfBoundsException canNotFindChunk) {}
        return null;
    }

    public Cursor<T> next(Cursor<T> cursor) throws IOException {

        try {
            long beginPosition = chunkList.first().beginPosition();
            if (cursor.offset() < beginPosition) cursor = cursor.skipTo(beginPosition);// TODO Class cast problem.
            return chunkList.chunkIn(cursor.offset()).next(cursor);
        } catch (ArrayIndexOutOfBoundsException e) { // over end of list
        } catch (IndexOutOfBoundsException e) {} // empty list
        return cursor.tail();
    }

    public long garbageCollect(long begin, long end) throws IOException {
        return garbageCollector.collect(begin, end);
    }

    public void flush() throws IOException {
        if (!chunkList.isEmpty()) chunkList.last().flush();
    }

    @Override
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        return chunkList.last().validateOrRecoverBy(validator);
    }

    @Override
    public void close() throws IOException { chunkList.close(); }

    private ArrayList<Chunk<T>> loadExistChunksBy(File baseDir, final ChunkFactory<T> chunkFactory) throws IOException {
        baseDir.mkdirs();
        checkArgument(baseDir.isDirectory(), "%s should be a directory", baseDir);
        return new NumberNamedFilesLoader<Chunk<T>>(baseDir, new FileHandler<Chunk<T>>() {
            @Override
            public Chunk<T> handle(File file, boolean last) throws IOException {
                return last ? chunkFactory.appendableChunkOn(file) : chunkFactory.readOnlyChunkOn(file);
            }

        }).loadTo(new ArrayList<Chunk<T>>());
    }

    private Chunk<T> grow() throws IOException {
        Chunk<T> chunk;
        if (chunkList.isEmpty()) chunk = chunkFactory.newFirstAppendableChunk();
        else {
            chunk = chunkFactory.newAppendableAfter(chunkList.last());
            convertLastRecentlyUsedChunkToReadOnly();
        }
        chunkList.append(chunk);
        return chunk;
    }

    private void convertLastRecentlyUsedChunkToReadOnly() throws IOException {
        chunkList.set(chunkList.lastIndex(), chunkList.last().asReadOnly());
    }

    public static interface Builder<T> extends BuilderConvention {

        @ArgumentIndex(0)
        @NotNull
        Builder<T> dir(File dir);

        @ArgumentIndex(1)
        @NotNull
        Builder<T> accessor(Accessor<T> value);

        @ArgumentIndex(2)
        @DefaultValue("4096")
        @GreaterThanOrEqual("4096")
        Builder<T> maximizeChunkCapacity(int value);

        @ArgumentIndex(3)
        @DefaultValue("4096")
        @GreaterThanOrEqual("4096")
        Builder<T> minimizeCollectLength(long value);

        @ArgumentIndex(4)
        @DefaultValue("4000")
        @GreaterThanOrEqual("1000")
        Builder<T> maxChunkIdleTimeMillis(long value);

        IPage<T> build();

    }
}
