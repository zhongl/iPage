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

package com.github.zhongl.journal1;

import com.github.zhongl.codec.Codec;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Pages implements Closable {

    private final LinkedList<Page> list;
    private final File dir;
    private final Codec codec;
    private final int pageCapacity;
    private final DeletingCallback deletingCallback;

    public Pages(File dir, Codec codec, int pageCapacity) {
        this.dir = dir;
        this.codec = codec;
        this.pageCapacity = pageCapacity;
        list = loadOrInitialize();
        deletingCallback = new DeletingCallback() {
            @Override
            public void onDelete(Page page) { list.remove(page); }
        };
    }

    private LinkedList<Page> loadOrInitialize() {
        LinkedList<Page> result = new LinkedList<Page>();
        checkArgument(dir.list().length == 0, "TODO load exist pages.");

        if (result.isEmpty()) {
            result.add(newPage(dir, codec, pageCapacity, 0L, new DeletingCallback() {
                @Override
                public void onDelete(Page page) {
                    list.remove(page);
                }
            })); // initialize
        }

        return result;  // TODO loadOrInitialize
    }

    protected abstract Page newPage(File dir, Codec codec, int pageCapacity, long number, DeletingCallback deletingCallback);

    /**
     * Append any object which the {@link com.github.zhongl.codec.Codec} supported.
     *
     * @param object
     * @param force
     *
     * @return appended position.
     */
    public Record append(Object object, boolean force) {
        return list.getLast().append(object, force, new OverflowCallback<Object>() {

            @Override
            public void onOverflow(Object rest, boolean force) {
                Page newPage = newPage(list.getLast().range().tail());
                newPage.append(rest, force, new OverflowCallback<Object>() {
                    @Override
                    public void onOverflow(Object rest, boolean force) {
                        throw new IllegalStateException("Object is too big to append to new page.");
                    }
                });
                list.addLast(newPage);
            }
        });
    }

    public List<Record> append(List<Object> objects, boolean force) {
        return list.getLast().append(objects, force, new OverflowCallback<List<Object>>() {

            @Override
            public void onOverflow(List<Object> rest, boolean force) {
                Page newPage = newPage(list.getLast().range().tail());
                newPage.append(rest, force, new OverflowCallback<List<Object>>() {
                    @Override
                    public void onOverflow(List<Object> rest, boolean force) {
                        throw new IllegalStateException("Objects is too big to append to new page.");
                    }
                });
                list.addLast(newPage);
            }
        });
    }

    public void reset() {
        range().remove();
        list.addLast(newPage(0L));
    }

    @Override
    public void close() {
        for (Page page : list) page.close();
    }

    public Range range() {
        long head = list.getFirst().range().head();
        long tail = list.getLast().range().tail();
        return new Range(head, tail) {
            @Override
            public Record record(long offset) {
                return null;  // TODO record
            }

            @Override
            public Range head(long offset) {
                return null;  // TODO head
            }

            @Override
            public Range tail(long offset) {
                return null;  // TODO tail
            }

            @Override
            public void remove() {
                // TODO remove
            }
        };
    }

    private Page newPage(long number) {
        return newPage(dir, codec, pageCapacity, number, deletingCallback);
    }
}
