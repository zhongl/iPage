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

package com.github.zhongl.ipage;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Cursor<T> {
    private final long offset;
    private final T lastValue;
    private final boolean end;

    private Cursor(long offset, T lastValue, boolean end) {
        this.offset = offset;
        this.lastValue = lastValue;
        this.end = end;
    }

    public static <T> Cursor<T> begin(long offset) {
        return new Cursor(offset, null, false);
    }

    public static <T> Cursor<T> cursor(long offset, T lastValue) {
        return new Cursor(offset, lastValue, false);
    }

    public <T> Cursor<T> end() {
        return new Cursor(offset, null, true);
    }

    public long offset() {
        return offset;
    }

    public T lastValue() {
        return lastValue;
    }

    public boolean isEnd() {
        return end;
    }
}
