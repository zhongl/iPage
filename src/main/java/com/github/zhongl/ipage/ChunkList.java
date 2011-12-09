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

import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;

import java.io.File;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.github.zhongl.ipage.Builder.ChunkFactory;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class ChunkList<T> {
    private final File baseDir;
    private final ChunkFactory<T> chunkFactory;
    private final LinkedList<Chunk<T>> chunks;
    private final ChunkOffsetRangeList chunkOffsetRangeList;

    public ChunkList(File baseDir, ChunkFactory<T> chunkFactory) throws IOException {
        this.baseDir = baseDir;
        chunks = new LinkedList<Chunk<T>>();
        this.chunkFactory = chunkFactory;
        chunkOffsetRangeList = new ChunkOffsetRangeList();
        loadExistChunks();
    }

    public Chunk<T> last() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.getLast();
    }

    public Chunk<T> grow() throws IOException {
        long beginPositionInIPage = chunks.isEmpty() ? 0L : last().endPositionInIPage() + 1;
        File file = new File(baseDir, beginPositionInIPage + "");
        convertLastRecentlyUsedChunkToReadOnly();
        Chunk<T> chunk = chunkFactory.create(beginPositionInIPage, file);
        chunks.addLast(chunk);
        return chunk;
    }

    public Chunk<T> chunkIn(long offset) throws IOException {
        if (chunks.isEmpty()) grow();
        return chunks.get(indexOfChunkIn(offset));
    }

    private int indexOfChunkIn(long offset) {return Range.binarySearch(chunkOffsetRangeList, offset);}

    public Chunk<T> first() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.getFirst();
    }

    public void close() throws IOException {
        for (Chunk<T> chunk : chunks) chunk.close();
    }

    private void convertLastRecentlyUsedChunkToReadOnly() throws IOException {
        if (chunks.isEmpty()) return;
        chunks.addLast(chunks.removeLast().asReadOnly());
    }

    private void loadExistChunks() throws IOException {
        File[] files = baseDir.listFiles(new NumberFileNameFilter());
        if (files == null) return;
        Arrays.sort(files, new FileNumberNameComparator());
        for (File file : files) {
            long beginPositionInIPage = Long.parseLong(file.getName());
            chunks.addLast(chunkFactory.create(beginPositionInIPage, file)); // make sure the appending chunk at last.
        }
    }

    public long garbageCollect(long begin, long end) throws IOException {
        int indexOfBeginChunk = indexOfChunkIn(begin);
        int indexOfEndChunk = indexOfChunkIn(end);
        if (indexOfBeginChunk == indexOfEndChunk) { // collectIn in one chunk
            return collectIn(indexOfBeginChunk, begin, end);
        } else {
            return collectBetween(indexOfBeginChunk, indexOfEndChunk, begin, end);
        }
    }

    private long collectBetween(int indexOfBeginChunk, int indexOfEndChunk, long begin, long end) throws IOException {
        Chunk<T> left = chunks.remove(indexOfBeginChunk);
        left = left.left(begin);

        if (left != null)
            chunks.add(indexOfBeginChunk, left);
        else
            indexOfEndChunk--; // decrease index because no add

        Chunk<T> right = chunks.remove(indexOfEndChunk);
        right = right.rightAndErase(end);
        chunks.add(indexOfEndChunk, right);

        return end - begin;
    }

    private long collectIn(int indexOfChunk, long begin, long end) throws IOException {
        Chunk<T> splittingChunk = chunks.remove(indexOfChunk);
        List<Chunk<T>> pieces = splittingChunk.splitBy(begin, end);
        if (pieces.isEmpty()) return 0L; // too small interval to split
        for (int i = 0; i < pieces.size(); i++) {
            chunks.add(indexOfChunk + i, pieces.get(i));
        }
        return end - begin;
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
