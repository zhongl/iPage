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

import com.github.zhongl.codec.Codec;
import com.github.zhongl.index.*;
import com.github.zhongl.page.*;
import com.github.zhongl.util.Entry;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Files;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Snapshot<V> {
    private final File headFile;
    private final File pagesDir;
    private final Indices indices;
    private final Binder<Entry<Key, V>> binder;
//    private final Set<String> fileNames;

    public Snapshot(File dir, final IndexCodec indexCodec, final Codec<Entry<Key, V>> entryCodec) {
        headFile = new File(dir, "HEAD");
        pagesDir = new File(dir, "pages");
//        fileNames = new HashSet<String>();

        try {

            FileParser parser = new FileParser() {

                @Override
                protected void bindBuilders() {
                    bind("P", new Builder() {
                        @Override
                        public Object build(Iterator<String> iterator) {
                            return new Page<Entry<Key, V>>(
                                    new File(pagesDir, iterator.next()),
                                    new Offset(iterator.next()),
                                    entryCodec
                            );
                        }
                    });

                    bind("I", new Builder() {
                        @Override
                        public Object build(Iterator<String> iterator) {
                            return new Indices(new File(pagesDir, iterator.next()), indexCodec);
                        }
                    });
                }
            };

            if (!pagesDir.exists()) {
                pagesDir.mkdirs();
                Files.write(("I\tnull.i\n").getBytes(), new File(pagesDir, "null.s"));
                new File(pagesDir, "null.i").createNewFile();
            }

            if (!headFile.exists()) Files.write("null.s".getBytes(), headFile);

            String snapshotFileName = Files.readFirstLine(headFile, Charset.defaultCharset());
            if (snapshotFileName != null) parser.parse(new File(pagesDir, snapshotFileName));

            this.indices = parser.getFirst(Indices.class);

            List<Page<Entry<Key, V>>> list = (List<Page<Entry<Key, V>>>) parser.getAll(Page.class);
            this.binder = new Binder<Entry<Key, V>>(pagesDir, list, entryCodec);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void defrag(
            Predicate<Element<Entry<Key, V>>> filter,
            Function<Element<Entry<Key, V>>, Void> collector
    ) throws IOException {
        binder.defrag(filter, collector);
    }

    public void append(
            Collection<Entry<Key, V>> values,
            Function<Element<Entry<Key, V>>, Void> collector
    ) throws IOException {
        binder.append(values, collector);
    }

    public void updateAndCleanUp() throws IOException {
        final Set<String> fileNames = new HashSet<String>();
        final StringBuilder sb = new StringBuilder();

        binder.foreachPage(new Function<Page<Entry<Key, V>>, Void>() {
            @Override
            public Void apply(Page<Entry<Key, V>> page) {
                sb.append("P").append('\t')
                  .append(page.fileName()).append('\t')
                  .append(page.number().toString()).append('\n');
                fileNames.add(page.fileName());
                return null;
            }
        });

        sb.append("I").append('\t').append(indices.fileName()).append('\n');
        fileNames.add(indices.fileName());

        File snapshotFile = new File(pagesDir, System.nanoTime() + ".s");
        fileNames.add(snapshotFile.getName());

        Files.write(sb.toString().getBytes(), snapshotFile);
        Files.write(snapshotFile.getName().getBytes(), headFile);

        File[] files = pagesDir.listFiles();
        for (File file : files) if (!fileNames.contains(file.getName())) file.delete();
        fileNames.clear();
    }

    public V get(Key key) {
        Index index = indices.get(key);
        if (index == null) return null;
        return index.get(new Function<Range, V>() {
            @Override
            public V apply(Range range) {
                try {
                    return binder.get(range).value();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        });

    }

    public Iterator<V> iterator() {
        return new AbstractIterator<V>() {
            final Iterator<Element<Entry<Key, V>>> iterator = binder.iterator();

            @Override
            protected V computeNext() {
                while (true) {
                    if (!iterator.hasNext()) return endOfData();
                    final Element<Entry<Key, V>> element = iterator.next();
                    if (isRemoved(element)) continue;
                    return element.value().value();
                }
            }
        };

    }

    public boolean isRemoved(final Element<Entry<Key, V>> element) {
        Index index = indices.get(element.value().key());
        if (index == null) return true;
        return index.get(new Function<Range, Boolean>() {

            @Override
            public Boolean apply(@Nullable Range range) {
                return !element.range().equals(range);
            }
        });

    }

    public void merge(Difference difference) throws IOException { indices.merge(difference); }

    public int aliveSize() { return indices.size(); }

    public long diskOccupiedBytes() {
        return binder.diskOccupiedBytes() + indices.diskOccupiedBytes();
    }
}
