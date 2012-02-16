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

package com.github.zhongl.ipage;

import java.util.Collections;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class Binder {
    protected final List<Page> pages;

    protected Binder(List<Page> pages) {this.pages = pages;}

    protected Page binarySearch(Number number) {
        int i = Collections.binarySearch(pages, new Numbered(number) {});
        i = i < 0 ? -(i + 2) : i;
        return pages.get(i);
    }
}
