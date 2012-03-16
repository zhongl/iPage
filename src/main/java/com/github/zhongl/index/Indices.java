/*
 * Copyright 2012 zhongl
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
    private final SortedList<Index> indexList;
    private final SortedList<Key> keyList;

    public Indices(File file, IndexCodec codec) {
        try {
            this.directBuffer = new DirectBuffer().loadFrom(file);
            this.codec = codec;
            this.merger = new Merger(file.getParentFile(), codec);
            this.keyList = new KeyList();
            this.indexList = new IndexList();
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public Index get(Key key) {
        int i = Collections.binarySearch(keyList, key);
        if (i < 0) return null;
        return indexList.get(i);
    }

    public Indices merge(Difference difference) throws IOException {
        IndicesFile indicesFile = merger.merge(peekingIterator(iterator()), peekingIterator(difference.iterator()));
        directBuffer.loadFrom(indicesFile.toFile());
        return this;
    }

    public Iterator<Index> iterator() { return indexList.iterator(); }

    public int size() { return indexList.size(); }

    public String fileName() { return directBuffer.backendFile().getName(); }

    public long diskOccupiedBytes() { return directBuffer.backendFile().length(); }

    private abstract class SortedList<T> extends AbstractList<T> implements RandomAccess {
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

    private class IndexList extends SortedList<Index> {

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

    }

    private class KeyList extends SortedList<Key> {
        @Override
        public Key get(final int index) {
            return directBuffer.read(new Function<ByteBuffer, Key>() {
                @Override
                public Key apply(ByteBuffer byteBuffer) {
                    byteBuffer.limit((index + 1) * codec.length()).position(index * codec.length());
                    return codec.decodeKey(byteBuffer);
                }
            });
        }
    }
}
