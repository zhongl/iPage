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

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Cursor implements Comparable<Cursor> {
    final long offset;

    Cursor(long offset) {
        checkArgument(offset >= 0);
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cursor)) return false;
        if (offset != ((Cursor) o).offset) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return (int) (offset ^ (offset >>> 32));
    }

    @Override
    public int compareTo(Cursor that) {
        return (int) (this.offset - that.offset);
    }

    public Cursor forword(int length) {
        return new Cursor(offset + length);
    }
}
