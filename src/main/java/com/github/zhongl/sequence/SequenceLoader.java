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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.ReadOnlyChannels;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class SequenceLoader<T> {
    private final File dir;
    private final int pageCapacity;
    private final Cursor lastSequenceTail;
    final Accessor<T> accessor;

    public SequenceLoader(File dir, Accessor<T> accessor, int pageCapacity, Cursor lastSequenceTail) {
        this.dir = dir;
        this.accessor = accessor;
        this.pageCapacity = pageCapacity;
        this.lastSequenceTail = lastSequenceTail;
    }

    LinkedList<LinkedPage<T>> load() throws IOException {
        final ReadOnlyChannels readOnlyChannels = new ReadOnlyChannels();
        if (!dir.exists()) dir.mkdirs();
        LinkedList<LinkedPage<T>> list = new FilesLoader<LinkedPage<T>>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<LinkedPage<T>>() {
                    @Override
                    public LinkedPage<T> transform(File file, boolean last) throws IOException {
                        long begin = Long.parseLong(file.getName());
                        if (lastSequenceTail.compareTo(new Cursor(begin)) < 0)
                            return new LinkedPage<T>(file, accessor, readOnlyChannels);
                        file.delete(); // delete invalid page file.
                        return null;
                    }
                }).loadTo(new LinkedList<LinkedPage<T>>());

        if (list.isEmpty()) {
            list.addLast(new LinkedPage<T>(new File(dir, "0"), accessor, pageCapacity, readOnlyChannels));
        } else {
            list.addLast(list.getLast().multiply());
        }
        return list;

    }

}
