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

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class ChunkList<T> {
    private final ArrayList<Chunk<T>> chunks;
    private final ChunkOffsetRangeList chunkOffsetRangeList;

    public ChunkList(ArrayList<Chunk<T>> chunks) {
        this.chunks = chunks;
        chunkOffsetRangeList = new ChunkOffsetRangeList();
    }

    public Chunk<T> last() throws IOException { return get(lastIndex()); }

    public int lastIndex() { return chunks.size() - 1; }

    public boolean append(Chunk<T> chunk) {return chunks.add(chunk);}

    public boolean isEmpty() {return chunks.isEmpty();}

    public Chunk<T> chunkIn(long offset) throws IOException { return get(indexOfChunkIn(offset)); }

    public int indexOfChunkIn(long offset) {return Range.binarySearch(chunkOffsetRangeList, offset);}

    public Chunk<T> first() throws IOException { return get(firstIndex()); }

    public void close() throws IOException { for (Chunk<T> chunk : chunks) chunk.close(); }

    public Chunk<T> get(int index) {return chunks.get(index);}

    public Chunk<T> set(int index, Chunk<T> chunk) {return chunks.set(index, chunk);}

    public Chunk<T> remove(int index) {return chunks.remove(index);}

    public void insert(int index, Chunk<T> chunk) {chunks.add(index, chunk);}

    private int firstIndex() {return 0;}

    private class ChunkOffsetRangeList extends AbstractList<Range> {

        @Override
        public Range get(int index) {
            Chunk<T> chunk = ChunkList.this.get(index);
            return new Range(chunk.beginPosition(), chunk.endPosition());
        }

        @Override
        public int size() { return chunks.size(); }

    }

}
