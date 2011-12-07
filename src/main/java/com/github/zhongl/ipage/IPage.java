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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage<T> implements Closeable {

    private final File baseDir;
    private final IPageBuilder<T>.ChunkFactory chunkFactory;
    private final List<Chunk> chunks; // TODO use LRU to cache chunks
    private final AbstractList<Range> chunkOffsetRangeList;

    public static IPageBuilder baseOn(File dir) {
        return new IPageBuilder(dir);
    }

    IPage(File baseDir, IPageBuilder<T>.ChunkFactory chunkFactory, List<Chunk> chunks) {
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

    private Chunk lastRecentlyUsedChunk() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.get(0);
    }

    private Chunk grow() throws IOException {
        long beginPositionInIPage = chunks.isEmpty() ? 0L : lastRecentlyUsedChunk().endPositionInIPage() + 1;
        File file = new File(baseDir, beginPositionInIPage + "");
        Chunk chunk = chunkFactory.create(beginPositionInIPage, file);
        chunks.add(0, chunk);
        return chunk;
    }

    public T get(long offset) throws IOException {
        if (chunks.isEmpty()) return null;
        releaseChunkIfNecessary();
        return (T) chunkIn(offset).get(offset);
    }

    private Chunk chunkIn(long offset) {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        return chunks.get(index);
    }

    public void flush() throws IOException {
        lastRecentlyUsedChunk().flush();
        releaseChunkIfNecessary();
    }

    @Override
    public void close() throws IOException {
        for (Chunk chunk : chunks) {
            chunk.close();
        }
    }

    /**
     * Remove part before the offset.
     *
     * @param offset
     */
    public void truncate(long offset) throws IOException {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        Chunk toTruncateChunk = chunks.get(index);
        List<Chunk> toRmoved = chunks.subList(index + 1, chunks.size());
        for (Chunk chunk : toRmoved) {
            chunk.erase();
        }
        toRmoved.clear();
        chunks.add(toTruncateChunk.dimidiate(offset).right());
    }

    public void recover() throws IOException {
        lastRecentlyUsedChunk().recover();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("IPage");
        sb.append("{baseDir=").append(baseDir);
        sb.append(", chunkCapacity=").append(chunkFactory);
        sb.append(", chunks=").append(chunks);
        sb.append('}');
        return sb.toString();
    }

    private class ChunkOffsetRangeList extends AbstractList<Range> {
        @Override
        public Range get(int index) {
            Chunk chunk = chunks.get(index);
            return new Range(chunk.beginPositionInIPage(), chunk.endPositionInIPage());
        }

        @Override
        public int size() {
            return chunks.size();
        }
    }
}
