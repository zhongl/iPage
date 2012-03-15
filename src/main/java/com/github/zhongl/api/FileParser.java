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

import com.google.common.base.Splitter;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class FileParser {
    private final Map<String, Builder> map = new HashMap<String, Builder>();
    private final List<Object> list = new ArrayList<Object>();

    public void parse(File file) throws IOException {
        bindBuilders();
        List<String> lines = Files.readLines(file, Charset.defaultCharset());
        for (String line : lines) {
            Iterator<String> iterator = Splitter.on('\t').omitEmptyStrings().trimResults().split(line).iterator();
            list.add(map.get(iterator.next()).build(iterator));
        }
    }

    public List<?> getAll(final Class<?> aClass) {
        List aList = new ArrayList();
        for (Object o : list) if (o.getClass().equals(aClass)) aList.add(o);
        return aList;
    }

    public <T> T getFirst(final Class<T> aClass) {
        for (Object o : list) if (o.getClass().equals(aClass)) return (T) o;
        return null;
    }

    protected abstract void bindBuilders();

    protected void bind(String token, Builder builder) {
        map.put(token, builder);
    }

    interface Builder {
        Object build(Iterator<String> iterator);
    }
}
