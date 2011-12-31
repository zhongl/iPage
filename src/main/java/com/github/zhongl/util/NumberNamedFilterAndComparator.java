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
import java.util.regex.Pattern;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class NumberNamedFilterAndComparator implements FilterAndComparator {
    private static final Pattern NUMBER_NAMED = Pattern.compile("[0-9]+");

    @Override
    public int compare(File o1, File o2) {
        return (int) (Long.parseLong(o1.getName()) - Long.parseLong(o2.getName()));
    }

    @Override
    public boolean accept(File dir, String name) {
        return NUMBER_NAMED.matcher(name).matches();
    }
}
