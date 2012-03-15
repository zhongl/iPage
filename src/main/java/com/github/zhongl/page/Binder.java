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
import com.github.zhongl.io.FileAppender;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.unmodifiableList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Binder<V> implements Iterable<Element<V>> {
    protected static final String SUFFIX = ".p";
    protected final File dir;
    protected final Codec<V> codec;
    protected final AtomicReference<List<Page<V>>> pages;

    public Binder(final File dir, List<Page<V>> list, final Codec<V> codec) {
        this.dir = dir;
        this.codec = codec;
        pages = new AtomicReference<List<Page<V>>>(unmodifiableList(list));
    }

    public V get(Range range) throws IOException { return binarySearch(new Offset(range.from())).get(range); }

    public Binder<V> append(final Collection<V> values, final Function<Element<V>, Void> collector) throws IOException {
        return modifyPages(new Modification<V>() {

            @Override
            public List<Page<V>> apply(List<Page<V>> list) throws IOException {
                long position = endPositionOf(list);
                ArrayList<Page<V>> newList = new ArrayList<Page<V>>(list);
                newList.add(new Page<V>(append(values, collector, position), new Offset(position), codec));
                return unmodifiableList(newList);
            }
        });
    }

    public Binder<V> defrag(final Predicate<Element<V>> filter, final Function<Element<V>, Void> collector) throws IOException {
        return modifyPages(new Modification<V>() {
            @Override
            public List<Page<V>> apply(List<Page<V>> list) throws IOException {
                long offset = 0;
                FileAppender fileAppender = new FileAppender(new File(dir, System.nanoTime() + SUFFIX));
                for (Page<V> page : list) {
                    RangeJoiner joiner = new RangeJoiner();
                    for (Element<V> element : page) {
                        if (filter.apply(element)) {
                            Range range = element.range();
                            joiner.join(range);
                            long length = range.to() - range.from();
                            collector.apply(new Element<V>(element.value(), new Range(offset, offset + length)));
                            offset += length;
                        }
                    }
                    page.transferTo(fileAppender, joiner);
                }
                File file = fileAppender.force();
                if (file.length() == 0) return Collections.emptyList();
                return Collections.singletonList(new Page<V>(file, new Offset(0L), codec));
            }
        });
    }

    @Override
    public Iterator<Element<V>> iterator() { return Iterables.concat(pages.get()).iterator(); }

    public long diskOccupiedBytes() { return endPositionOf(pages.get()); }

    private long endPositionOf(List<Page<V>> list) {
        return list.isEmpty() ? 0L : list.get(list.size() - 1).nextPageNumber().value();
    }

    private Binder<V> modifyPages(Modification<V> modification) throws IOException {
        while (true) {
            List<Page<V>> list = pages.get();
            if (pages.compareAndSet(list, unmodifiableList(modification.apply(list)))) break;
        }
        return this;
    }

    private File append(Iterable<V> values, Function<Element<V>, Void> collector, long offset) throws IOException {
        FileAppender fileAppender = new FileAppender(new File(dir, System.nanoTime() + SUFFIX));
        for (V value : values) offset += append(value, fileAppender, collector, offset);
        return fileAppender.force();
    }

    private long append(V value, FileAppender fileAppender, Function<Element<V>, Void> collector, long offset) throws IOException {
        int appended = fileAppender.append(codec.encode(value));
        collector.apply(new Element<V>(value, new Range(offset, offset + appended)));
        return appended;
    }

    private Page<V> binarySearch(Offset offset) {
        int i = Collections.binarySearch(pages.get(), new Numbered<Offset>(offset) {});
        i = i < 0 ? -(i + 2) : i; // round index
        return pages.get().get(i);
    }

    public void foreachPage(Function<Page<V>, Void> function) {
        for (Page<V> page : pages.get()) function.apply(page);
    }

    private interface Modification<V> {
        List<Page<V>> apply(List<Page<V>> list) throws IOException;
    }
}
