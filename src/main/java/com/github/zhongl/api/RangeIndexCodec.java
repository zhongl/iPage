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

package com.github.zhongl.api;

import com.github.zhongl.index.Index;
import com.github.zhongl.index.IndexCodec;
import com.github.zhongl.index.Key;
import com.github.zhongl.index.KeyCodec;
import com.github.zhongl.page.Range;
import com.google.common.base.Function;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RangeIndexCodec implements IndexCodec, IndexFactory {
    private final KeyCodec keyCodec;

    public RangeIndexCodec(KeyCodec keyCodec) {
        this.keyCodec = keyCodec;
    }

    @Override
    public Index decode(ByteBuffer byteBuffer) {
        return index(keyCodec.decode(byteBuffer), new Range(byteBuffer.getLong(), byteBuffer.getLong()));
    }

    @Override
    public ByteBuffer encode(Index value) {
        final ByteBuffer buffer = ByteBuffer.allocate(length()).put(keyCodec.encode(value.key()));

        value.get(new Function<Range, Void>() {
            @Override
            public Void apply(Range range) {
                buffer.putLong(range.from()).putLong(range.to());
                return null;
            }
        });
        return (ByteBuffer) buffer.flip();
    }

    @Override
    public int length() {
        return keyCodec.length() + 16;
    }

    @Override
    public Index removedIndex(Key key) {
        return new Index(key) {
            @Override
            public boolean isRemoved() {
                return true;
            }

            @Override
            public <Clue, Value> Value get(Function<Clue, Value> function) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public Index index(Key key, Range range) {
        return new RangeIndex(key, range);
    }

    private static class RangeIndex extends Index {
        private final Range range;

        public RangeIndex(Key key, Range range) {
            super(key);
            this.range = range;
        }

        @Override
        public boolean isRemoved() { return false; }

        @Override
        public <Clue, Value> Value get(Function<Clue, Value> function) { return function.apply((Clue) range); }
    }
}
