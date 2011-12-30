/*
 * Copyright 2011 zhongl
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
import com.github.zhongl.util.FileHandler;
import com.github.zhongl.util.NumberNamedFilesLoader;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

public class LinkedPageLoader<T> {

    private final File dir;
    private final Accessor<T> accessor;
    private final ReadOnlyChannels readOnlyChannels;
    private final int pageCapacity;

    public LinkedPageLoader(File dir, Accessor<T> accessor, int pageCapacity) {
        this.dir = dir;
        this.accessor = accessor;
        this.pageCapacity = pageCapacity;
        readOnlyChannels = new ReadOnlyChannels();
    }

    public LinkedList<LinkedPage<T>> load() throws IOException {
        LinkedList<LinkedPage<T>> list = new NumberNamedFilesLoader<LinkedPage<T>>(dir, new FileHandler<LinkedPage<T>>() {
            @Override
            public LinkedPage<T> handle(File file, boolean last) throws IOException {
                try {
                    return new LinkedPage<T>(file, accessor, readOnlyChannels);
                } catch (IllegalStateException e) {
                    file.delete();
                    return null;
                }

            }
        }).loadTo(new LinkedList<LinkedPage<T>>());

        if (list.isEmpty()) {
            list.addLast(new LinkedPage<T>(new File(dir, "0"), accessor, pageCapacity, readOnlyChannels));
        }
        return list;
    }
}