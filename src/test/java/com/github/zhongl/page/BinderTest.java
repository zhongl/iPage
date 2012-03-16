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
import com.github.zhongl.util.FileTestContext;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.zhongl.util.IteratorAsserts.assertIteratorOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BinderTest extends FileTestContext {

    private Binder<Integer> binder;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        dir = testDir("usage");

        binder = new Binder<Integer>(dir, Collections.<Page<Integer>>emptyList(), new Codec<Integer>() {
            @Override
            public Integer decode(ByteBuffer buffer) {
                return buffer.getInt();
            }

            @Override
            public ByteBuffer encode(Integer integer) {
                return (ByteBuffer) ByteBuffer.allocate(4).putInt(integer).flip();
            }

            @Override
            public int encode(Integer value, ByteBuffer byteBuffer) {
                byteBuffer.putInt(value);
                return 4;
            }
        });
    }

    @Test
    public void usage() throws Exception {

        final List<Element<Integer>> collector = new ArrayList<Element<Integer>>();

        binder = binder.append(Arrays.asList(1, 2, 3), new Function<Element<Integer>, Void>() {
            @Override
            public Void apply(@Nullable Element<Integer> element) {
                collector.add(element);
                return null;
            }
        });

        assertThat(collector, is(Arrays.asList(
                element(1, range(0, 4)),
                element(2, range(4, 8)),
                element(3, range(8, 12))
        )));

        assertThat(binder.get(range(0, 4)), is(1));
        assertThat(binder.get(range(4, 8)), is(2));
        assertThat(binder.get(range(8, 12)), is(3));


        binder = binder.append(Arrays.asList(4, 5, 6), new Function<Element<Integer>, Void>() {
            @Override
            public Void apply(Element<Integer> element) { return null; }
        });


        assertIteratorOf(binder,
                element(1, range(0, 4)),
                element(2, range(4, 8)),
                element(3, range(8, 12)),
                element(4, range(12, 16)),
                element(5, range(16, 20)),
                element(6, range(20, 24))
        );

        collector.clear();

        binder = binder.defrag(
                new Predicate<Element<Integer>>() {
                    @Override
                    public boolean apply(@Nullable Element<Integer> element) {
                        return element.value() > 4;
                    }
                },
                new Function<Element<Integer>, Void>() {
                    @Override
                    public Void apply(@Nullable Element<Integer> element) {
                        collector.add(element);
                        return null;
                    }
                }
        );

        assertThat(collector, is(Arrays.asList(
                element(5, range(0, 4)),
                element(6, range(4, 8))
        )));

        assertIteratorOf(binder,
                element(5, range(0, 4)),
                element(6, range(4, 8)));
    }


    static <T> Element<T> element(T value, Range range) {
        return new Element(value, range);
    }

    static Range range(long from, long to) {
        return new Range(from, to);
    }
}
