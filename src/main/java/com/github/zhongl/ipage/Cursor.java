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

    private final long position;
    private final T lastValue;
    private final boolean tail;

    protected Cursor(long position, T lastValue, boolean tail) {
        this.position = position;
        this.lastValue = lastValue;
        this.tail = tail;
    }

    public static <T> Cursor<T> head() {
        return new Cursor<T>(0L, null, false);
    }

    public Cursor<T> forword(long length, T lastValue) {
        return new Cursor(position + length, lastValue, false);
    }

    public Cursor<T> tail() {
        return new Cursor(position, null, true);
    }

    public long offset() {
        return position;
    }

    public T lastValue() {
        return lastValue;
    }

    public boolean isTail() {
        return tail;
    }

    public Cursor<T> skipTo(long position) {
        return new Cursor(position, null, false);
    }
}
