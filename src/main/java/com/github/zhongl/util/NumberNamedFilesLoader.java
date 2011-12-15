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

package com.github.zhongl.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class NumberNamedFilesLoader<V> {
    private final File dir;
    private final FileHandler<V> handler;

    public NumberNamedFilesLoader(File dir, FileHandler<V> handler) {
        this.dir = dir;
        this.handler = handler;
    }

    public <T extends Collection<V>> T loadTo(T collection) throws IOException {
        File[] files = dir.listFiles(new NumberNameFilter());
        if (files == null) return collection;
        Arrays.sort(files, new FileNumberNameComparator());
        for (File file : files) {
            for (int i = 0; i < files.length; i++) {
                boolean last = i == files.length - 1;
                collection.add(handler.handle(files[i], last));
            }

        }
        return collection;
    }
}
