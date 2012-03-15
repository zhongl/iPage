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

package com.github.zhongl.index;

import com.google.common.base.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index implements Comparable<Index> {
    private final Key key;

    protected Index(Key key) { this.key = checkNotNull(key); }

    public Key key() { return key; }

    @Override
    public int compareTo(Index o) { return key.compareTo(checkNotNull(o).key()); }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return key.equals(index.key);
    }

    @Override
    public final int hashCode() { return key.hashCode(); }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "{key=" + key + ", removed=" + isRemoved() + '}';
    }

    public abstract boolean isRemoved();

    public abstract <Clue, Value> Value get(Function<Clue, Value> function);
}
