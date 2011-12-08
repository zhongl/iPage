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

import com.github.zhongl.integerity.ValidateOrRecover;
import com.github.zhongl.integerity.Validator;
import com.google.common.collect.AbstractIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage<T> implements Closeable, Iterable<T>, ValidateOrRecover<T, IOException> {

    private final File baseDir;
    private final IPageBuilder.ChunkFactory<T> chunkFactory;
    private final List<Chunk<T>> chunks; // TODO use LRU to cache chunks
    private final AbstractList<Range> chunkOffsetRangeList;

    public static <T> IPageBuilder<T> baseOn(File dir) {
        return new IPageBuilder<T>(dir);
    }

    IPage(File baseDir, IPageBuilder.ChunkFactory<T> chunkFactory, List<Chunk<T>> chunks) {
        this.baseDir = baseDir;
        this.chunkFactory = chunkFactory;
        this.chunks = chunks;
        chunkOffsetRangeList = new ChunkOffsetRangeList();
    }

    public long append(T record) throws IOException {
        try {
            releaseChunkIfNecessary();
            return lastRecentlyUsedChunk().append(record);
        } catch (OverflowException e) {
            grow();
            return append(record);
        }
    }

    private void releaseChunkIfNecessary() {
        // TODO releaseChunkIfNecessary
    }

    private Chunk<T> lastRecentlyUsedChunk() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.get(0);
    }

    private Chunk<T> grow() throws IOException {
        long beginPositionInIPage = chunks.isEmpty() ? 0L : lastRecentlyUsedChunk().endPositionInIPage() + 1;
        File file = new File(baseDir, beginPositionInIPage + "");
        Chunk<T> chunk = chunkFactory.create(beginPositionInIPage, file);
        chunks.add(0, chunk);
        return chunk;
    }

    public T get(long offset) throws IOException {
        if (chunks.isEmpty()) return null;
        releaseChunkIfNecessary();
        try {
            return chunkIn(offset).get(offset);
        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
    }

    private Chunk<T> chunkIn(long offset) {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        return chunks.get(index);
    }

    public void flush() throws IOException {
        lastRecentlyUsedChunk().flush();
        releaseChunkIfNecessary();
    }

    @Override
    public boolean validateOrRecoverBy(Validator<T, IOException> validator) throws IOException {
        return lastRecentlyUsedChunk().validateOrRecoverBy(validator);
    }

    @Override
    public Iterator<T> iterator() {
        LinkedList<Chunk<T>> copy = new LinkedList<Chunk<T>>(chunks);
        Collections.reverse(copy);

        new AbstractIterator<Chunk<T>>() {

            @Override
            protected Chunk<T> computeNext() {
                return null;  // TODO computeNext
            }
        };
        return new ChunkIterator<T>(copy);
    }

    @Override
    public void close() throws IOException {
        for (Chunk<T> chunk : chunks) {
            chunk.close();
        }
    }

    private class ChunkOffsetRangeList extends AbstractList<Range> {
        @Override
        public Range get(int index) {
            Chunk<T> chunk = chunks.get(index);
            return new Range(chunk.beginPositionInIPage(), chunk.endPositionInIPage());
        }

        @Override
        public int size() {
            return chunks.size();
        }
    }
}
