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

import com.github.zhongl.util.FileTestContext;
import com.google.common.collect.Iterators;
import org.junit.Test;

import java.util.Iterator;

import static com.google.common.collect.Iterators.peekingIterator;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class MergerTest extends FileTestContext {

    @Test
    public void merge() throws Exception {
        Index index1 = new TestIndex(1, true);
        Index index2 = new TestIndex(2, false);
        Index index3 = new TestIndex(3, false);
        Index index4 = new TestIndex(4, false);
        Index index5 = new TestIndex(5, false);

        dir = testDir("merge");

        TestIndexCodec codec = new TestIndexCodec();
        Merger merger = new Merger(dir, codec);

        IndicesFile indicesFile = merger.merge(
                peekingIterator(Iterators.<Index>forArray(
                        index1,
                        index3,
                        index5
                )),
                peekingIterator(Iterators.<Index>forArray(
                        index2,
                        new TestIndex(3, true),
                        index4
                )));

        Indices indices = new Indices(indicesFile.toFile(), codec);

        Iterator<Index> iterator = indices.iterator();
        assertThat(iterator.next(), is(index2));
        assertThat(iterator.next(), is(index4));
        assertThat(iterator.next(), is(index5));
        assertThat(iterator.hasNext(), is(false));
    }

}
