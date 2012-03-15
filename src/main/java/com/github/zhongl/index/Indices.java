/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.index;

import com.github.zhongl.io.DirectBuffer;
import com.google.common.base.Function;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.RandomAccess;

import static com.google.common.collect.Iterators.peekingIterator;


/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Indices {

    private final Merger merger;
    private final IndexCodec codec;
    private final DirectBuffer directBuffer;
    private final SortedIndexList sortedIndexList;

    public Indices(File file, IndexCodec codec) {
        try {
            this.directBuffer = new DirectBuffer().loadFrom(file);
            this.codec = codec;
            this.merger = new Merger(file.getParentFile(), codec);
            this.sortedIndexList = new SortedIndexList();
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Index get(Key key) {
        int i = Collections.binarySearch(sortedIndexList, new FakeIndex(key));
        if (i < 0) return null;
        return sortedIndexList.get(i);
    }

    public Indices merge(Difference difference) throws IOException {
        IndicesFile indicesFile = merger.merge(peekingIterator(iterator()), peekingIterator(difference.iterator()));
        directBuffer.loadFrom(indicesFile.toFile());
        return this;
    }

    public Iterator<Index> iterator() { return sortedIndexList.iterator(); }

    public int size() { return sortedIndexList.size(); }

    public String fileName() { return directBuffer.backendFile().getName(); }

    public long diskOccupiedBytes() { return directBuffer.backendFile().length(); }

    private static class FakeIndex extends Index {

        protected FakeIndex(Key key) { super(key); }

        @Override
        public boolean isRemoved() { return false; }

        @Override
        public <Clue, Value> Value get(Function<Clue, Value> function) { throw new UnsupportedOperationException(); }
    }

    private class SortedIndexList extends AbstractList<Index> implements RandomAccess {

        @Override
        public Index get(final int index) {
            return directBuffer.read(new Function<ByteBuffer, Index>() {
                @Override
                public Index apply(ByteBuffer byteBuffer) {
                    byteBuffer.limit((index + 1) * codec.length()).position(index * codec.length());
                    return codec.decode(byteBuffer);
                }
            });
        }

        @Override
        public int size() {
            return directBuffer.read(new Function<ByteBuffer, Integer>() {
                @Override
                public Integer apply(ByteBuffer byteBuffer) {
                    return byteBuffer.capacity() / codec.length();
                }
            });
        }
    }
}
