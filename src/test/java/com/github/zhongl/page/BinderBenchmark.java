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

package com.github.zhongl.page;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BinderBenchmark extends FileTestContext {

    public static final int LENGTH = 1024;
    private Codec<byte[]> codec;
    private List<Page<byte[]>> empty;
    private Function<Element<byte[]>, Void> collector;

    @Before
    public void setUp() throws Exception {
        codec = new Codec<byte[]>() {
            @Override
            public byte[] decode(ByteBuffer byteBuffer) {
                byte[] bytes = new byte[1024];
                byteBuffer.get(bytes);
                return bytes;
            }

            @Override
            public ByteBuffer encode(byte[] value) {
                return ByteBuffer.wrap(value);
            }
        };
        empty = Collections.emptyList();
        collector = new Function<Element<byte[]>, Void>() {
            @Override
            public Void apply(Element<byte[]> input) {
                return null;
            }
        };
    }

    @Test
    public void iterate() throws Exception {
        dir = testDir("iterate");
        final Binder<byte[]> binder = new Binder<byte[]>(dir, empty, codec);

        final int times = 1000000;
        Benchmarks.benchmark("append", new Runnable() {
            @Override
            public void run() {
                try {
                    binder.append(list(times), collector);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, times);

        Benchmarks.benchmark("iterate", new Runnable() {
            @Override
            public void run() {
                for (Element<byte[]> e : binder) {}
            }
        }, times);
    }

    @Test
    public void get() throws Exception {
        dir = testDir("get");
        final Binder<byte[]> binder = new Binder<byte[]>(dir, empty, codec);

        final int times = 1000000;
        Benchmarks.benchmark("append", new Runnable() {
            @Override
            public void run() {
                try {
                    binder.append(list(times), collector);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, times);

        Benchmarks.benchmark("get", new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < times; i++) {
                        binder.get(new Range(i * 1024, (i + 1) * 1024));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, times);
    }

    @Test
    public void defrag() throws Exception {
        dir = testDir("defrag");
        final Binder<byte[]> binder = new Binder<byte[]>(dir, empty, codec);

        final int times = 1000000;
        binder.append(list(times), collector);

        Benchmarks.benchmark("defrag", new Runnable() {
            @Override
            public void run() {
                try {
                    binder.defrag(new Predicate<Element<byte[]>>() {
                        @Override
                        public boolean apply(Element<byte[]> input) {
                            return true;
                        }
                    }, collector);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, times);
    }

    private AbstractList<byte[]> list(final int times) {
        return new AbstractList<byte[]>() {
            @Override
            public byte[] get(int index) {
                return new byte[LENGTH];
            }

            @Override
            public int size() {
                return times;
            }
        };
    }
}
