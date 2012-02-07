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

package com.github.zhongl.ex.journal;

import com.github.zhongl.ex.page.Number;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Checkpoint extends Number<Checkpoint> {

    private final Long value;

    public Checkpoint(long value) {
        this.value = value;
    }

    public Checkpoint(String text) {
        this(Long.parseLong(text));
    }

    public Checkpoint add(long delta) {
        return new Checkpoint(value + delta);
    }

    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public int compareTo(Checkpoint o) {
        return value.compareTo(o.value);
    }
}
