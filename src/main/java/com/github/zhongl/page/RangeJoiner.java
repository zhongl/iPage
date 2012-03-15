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

package com.github.zhongl.page;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;
import java.util.LinkedList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
class RangeJoiner implements Iterable<Range> {

    private LinkedList<Range> ranges;

    RangeJoiner() {
        ranges = new LinkedList<Range>();
    }

    public void join(Range range) {
        Range joined = range;
        if (!ranges.isEmpty()) {
            Range last = ranges.getLast();
            if (last.to() == range.from()) {
                ranges.removeLast();
                joined = new Range(last.from(), range.to());
            }
        }
        ranges.add(joined);
    }

    @Override
    public Iterator<Range> iterator() {
        return ranges.iterator();
    }
}
