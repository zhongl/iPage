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
import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;

import java.io.File;
import java.io.IOException;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class ChunkList<T> {
    private final File baseDir;
    private final LinkedList<Chunk<T>> chunks;
    private final ChunkOffsetRangeList chunkOffsetRangeList;
    private final int minimizeCollectLength;
    private final int capacity;
    private final Accessor<T> accessor;

    public ChunkList(File baseDir, int capacity, Accessor<T> accessor, int minimizeCollectLength) throws IOException {
        this.baseDir = baseDir;
        this.minimizeCollectLength = minimizeCollectLength;
        this.accessor = accessor;
        this.capacity = capacity;
        chunks = new LinkedList<Chunk<T>>();
        chunkOffsetRangeList = new ChunkOffsetRangeList();
        loadExistChunks();
    }

    public Chunk<T> last() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.getLast();
    }

    public Chunk<T> grow() throws IOException {
        long beginPosition = chunks.isEmpty() ? 0L : last().endPosition() + 1;
        File file = new File(baseDir, Long.toString(beginPosition));
        convertLastRecentlyUsedChunkToReadOnly();
        Chunk<T> chunk = Chunk.appendableChunk(file, beginPosition, capacity, accessor);
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
        chunks.addLast(Chunk.asReadOnly(chunks.removeLast(), minimizeCollectLength));
    }

    private void loadExistChunks() throws IOException {
        File[] files = baseDir.listFiles(new NumberFileNameFilter());
        if (files == null) return;
        Arrays.sort(files, new FileNumberNameComparator());

        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            long beginPosition = Long.parseLong(file.getName());
            Chunk<T> chunk;
            if (i == files.length - 1) {
                chunk = Chunk.appendableChunk(file, beginPosition, capacity, accessor);
            } else {
                chunk = Chunk.readOnlyChunk(file, beginPosition, (int) file.length(), accessor, minimizeCollectLength);
            }
            chunks.addLast(chunk);
        }
    }

    public long garbageCollect(long begin, long end) throws IOException {
        int indexOfBeginChunk = indexOfChunkIn(begin);
        int indexOfEndChunk = indexOfChunkIn(end);
        if (indexOfBeginChunk == indexOfEndChunk) { // collectIn in one chunk
            return collectIn(indexOfBeginChunk, begin, end);
        } else {
            return collectRight(indexOfEndChunk, end) + collectLeft(indexOfBeginChunk, begin);
        }
    }

    /** @see Chunk#right(long) */
    private long collectRight(int indexOfEndChunk, long end) throws IOException {
        Chunk<T> right = chunks.get(indexOfEndChunk);
        Chunk<T> newChunk = right.right(end);
        if (newChunk == right) return 0L;
        chunks.remove(indexOfEndChunk);
        chunks.add(indexOfEndChunk, newChunk);
        return end - right.beginPosition();
    }

    /** @see Chunk#left(long) */
    private long collectLeft(int indexOfBeginChunk, long begin) throws IOException {
        Chunk<T> left = chunks.remove(indexOfBeginChunk);
        Chunk<T> newLeft = left.left(begin);
        if (newLeft != null) chunks.add(indexOfBeginChunk, newLeft);
        return left.endPosition() + 1 - begin;
    }

    /** @see Chunk#split(long, long) */
    private long collectIn(int indexOfChunk, long begin, long end) throws IOException {
        Chunk<T> splittingChunk = chunks.get(indexOfChunk);
        List<Chunk<T>> pieces = splittingChunk.split(begin, end);
        if (pieces.isEmpty()) return 0L; // can't left appending chunk
        chunks.remove(indexOfChunk);
        for (int i = 0; i < pieces.size(); i++) {
            chunks.add(indexOfChunk + i, pieces.get(i));
        }
        return end - begin;
    }

    private class ChunkOffsetRangeList extends AbstractList<Range> {

        @Override
        public Range get(int index) {
            Chunk<T> chunk = chunks.get(index);
            return new Range(chunk.beginPosition(), chunk.endPosition());
        }

        @Override
        public int size() { return chunks.size(); }

    }
}
