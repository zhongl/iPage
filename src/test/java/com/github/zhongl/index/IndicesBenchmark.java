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

import com.github.zhongl.api.Md5Key;
import com.github.zhongl.api.Md5KeyCodec;
import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.Md5;
import com.google.common.base.Function;
import com.google.common.primitives.Ints;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.TreeSet;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndicesBenchmark extends FileTestContext {

    @Test
    public void get() throws Exception {
        dir = testDir("get");
        final Indices indices = new Indices(new File(dir, "null.i"), new InnerIndexCodec());

        Difference difference = new Difference(new TreeSet<Index>());
        final int times = 100000;

        for (int i = 0; i < times; i++) {
            difference.add(new InnerIndex(key(i)));
        }

        indices.merge(difference);

        Benchmarks.benchmark("get", new Runnable() {

            @Override
            public void run() {
                for (int i = 0; i < times; i++) {
                    indices.get(key(i));
                }

            }
        }, times);
    }

    private static Md5Key key(int value) {return new Md5Key(Md5.md5(Ints.toByteArray(value)));}

    private static class InnerIndexCodec implements IndexCodec {
        Md5KeyCodec codec = new Md5KeyCodec();

        @Override
        public Index decode(ByteBuffer byteBuffer) {
            return new InnerIndex(codec.decode(byteBuffer));
        }

        @Override
        public ByteBuffer encode(Index value) {
            return codec.encode(value.key());
        }

        @Override
        public int length() {
            return codec.length();
        }
    }

    private static class InnerIndex extends Index {
        public InnerIndex(Md5Key key) {
            super(key);
        }

        @Override
        public boolean isRemoved() { return false; }

        @Override
        public <Clue, Value> Value get(Function<Clue, Value> function) {
            throw new UnsupportedOperationException();
        }
    }
}
