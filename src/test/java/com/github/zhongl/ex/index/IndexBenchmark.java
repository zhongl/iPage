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

package com.github.zhongl.ex.index;

import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.util.Benchmarks;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class IndexBenchmark extends FileTestContext {

    public static final int SIZE = FlexIndex.MAX_ENTRY_SIZE * 2;

    @Test
    public void get() throws Exception {
        dir = testDir("get");
        final Index index = newIndex(dir);

        final Iterator<Entry<Md5Key, Offset>> sortedIterator1 = randomSortedIterator(0);
        Benchmarks.benchmark(getClass().getSimpleName() + " init merge", new Runnable() {
            @Override
            public void run() {
                try {
                    index.merge(sortedIterator1);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, SIZE);


        Benchmarks.benchmark(getClass().getSimpleName() + " get", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < SIZE; i++) {
                    index.get(Md5Key.generate(i + ""));
                }
            }
        }, SIZE);


        System.out.println(ReadOnlyMappedBuffers.stats());

        index.close();
    }

    @Test
    public void crossMerge() throws Exception {
        dir = testDir("crossMerge");
        final Index index = newIndex(dir);

        final Iterator<Entry<Md5Key, Offset>> sortedIterator1 = randomSortedIterator(0);
        Benchmarks.benchmark(getClass().getSimpleName() + " init merge", new Runnable() {
            @Override
            public void run() {
                try {
                    index.merge(sortedIterator1);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, SIZE);

        final Iterator<Entry<Md5Key, Offset>> sortedIterator2 = randomSortedIterator(FlexIndex.MAX_ENTRY_SIZE * 2 + 1);
        Benchmarks.benchmark(getClass().getSimpleName() + " cross merge", new Runnable() {
            @Override
            public void run() {
                try {
                    index.merge(sortedIterator2);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, SIZE);

        index.close();
    }

    protected abstract Index newIndex(File dir) throws IOException;

    private Iterator<Entry<Md5Key, Offset>> randomSortedIterator(int start) {
        List<Entry<Md5Key, Offset>> entries = new ArrayList<Entry<Md5Key, Offset>>(SIZE);
        for (int i = start; i < SIZE + start; i++) {
            Md5Key key = Md5Key.generate(i + "");
            entries.add(new Entry<Md5Key, Offset>(key, new Offset(i)));
        }
        Collections.sort(entries);
        return entries.iterator();
    }

}
