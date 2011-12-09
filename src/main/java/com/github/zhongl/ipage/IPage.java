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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.AbstractList;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage<T> implements Closeable, ValidateOrRecover<T, IOException> {

    private final File baseDir;
    private final Builder.ChunkFactory<T> chunkFactory;
    private final LinkedList<Chunk<T>> chunks; // TODO use LRU to cache chunks
    private final AbstractList<Range> chunkOffsetRangeList;

    public static <T> Builder<T> baseOn(File dir) {
        return new Builder<T>(dir);
    }

    IPage(File baseDir, Builder.ChunkFactory<T> chunkFactory, LinkedList<Chunk<T>> chunks) {
        this.baseDir = baseDir;
        this.chunkFactory = chunkFactory;
        this.chunks = chunks;
        chunkOffsetRangeList = new ChunkOffsetRangeList();
    }

    public long append(T record) throws IOException {
        try {
            releaseChunkIfNecessary();
            return lastRecentlyUsedChunk().append(record);
        } catch (BufferOverflowException e) {
            grow();
            return append(record);
        }
    }

    private void releaseChunkIfNecessary() {
        // TODO releaseChunkIfNecessary
    }

    private Chunk<T> lastRecentlyUsedChunk() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.getLast();
    }

    private Chunk<T> grow() throws IOException {
        long beginPositionInIPage = chunks.isEmpty() ? 0L : lastRecentlyUsedChunk().endPositionInIPage() + 1;
        File file = new File(baseDir, beginPositionInIPage + "");
        convertLastRecentlyUsedChunkToReadOnly();
        Chunk<T> chunk = chunkFactory.create(beginPositionInIPage, file);
        chunks.addLast(chunk);
        return chunk;
    }

    private void convertLastRecentlyUsedChunkToReadOnly() throws IOException {
        if (chunks.isEmpty()) return;
        chunks.addLast(chunks.removeLast().asReadOnly());
    }

    public T get(long offset) throws IOException {
        if (chunks.isEmpty()) return null;
        releaseChunkIfNecessary();
        try {
            return chunkIn(offset).get(offset);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    public Cursor<T> next(Cursor<T> cursor) throws IOException {
        try {
            long beginPosition = chunks.getFirst().beginPositionInIPage();
            if (cursor.offset() < beginPosition) cursor = Cursor.begin(beginPosition);
            return chunkIn(cursor.offset()).next(cursor);
        } catch (IndexOutOfBoundsException e) {
            return cursor.end();
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
    public void close() throws IOException {
        for (Chunk<T> chunk : chunks) chunk.close();
    }

    private class ChunkOffsetRangeList extends AbstractList<Range> {
        @Override
        public Range get(int index) {
            Chunk<T> chunk = chunks.get(index);
            return new Range(chunk.beginPositionInIPage(), chunk.endPositionInIPage());
        }

        @Override
        public int size() { return chunks.size(); }
    }
}
