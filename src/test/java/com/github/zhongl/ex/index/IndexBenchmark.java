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

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;
import com.github.zhongl.util.FileTestContext;
import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IndexBenchmark extends FileTestContext {

    @Test
    public void mergeAndGet() throws Exception {
        dir = testDir("mergeAndGet");
        final Index index = new Index(dir);

        final Iterator<Entry<Md5Key, Offset>> sortedIterator1 = randomSortedIterator(0);
        benchmark("init merge", new Runnable() {
            @Override
            public void run() {
                try {
                    index.merge(sortedIterator1);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        final Iterator<Entry<Md5Key, Offset>> sortedIterator2 = randomSortedIterator(100);
        benchmark("cross merge", new Runnable() {
            @Override
            public void run() {
                try {
                    index.merge(sortedIterator2);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        benchmark("get", new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < Index.MAX_ENTRY_SIZE * 10; i++) {
                    index.get(Md5Key.generate(i + ""));
                }
            }
        });

        index.close();
    }

    private void benchmark(String name, Runnable runnable) {
        Stopwatch stopwatch = new Stopwatch().start();
        runnable.run();
        stopwatch.stop();
        System.out.println(MessageFormat.format("{0} : {1}, {2, number}ns",
                name,
                stopwatch,
                stopwatch.elapsedTime(TimeUnit.NANOSECONDS) / Index.MAX_ENTRY_SIZE * 10));
    }

    private Iterator<Entry<Md5Key, Offset>> randomSortedIterator(int start) {
        List<Entry<Md5Key, Offset>> entries = new ArrayList<Entry<Md5Key, Offset>>(Index.MAX_ENTRY_SIZE * 10);
        for (int i = start; i < Index.MAX_ENTRY_SIZE * 10 + start; i++) {
            Md5Key key = Md5Key.generate(i + "");
            entries.add(new Entry<Md5Key, Offset>(key, new Offset(i)));
        }
        Collections.sort(entries);
        return entries.iterator();
    }

}
