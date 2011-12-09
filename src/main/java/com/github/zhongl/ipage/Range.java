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

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
class Range {

    private final long begin;
    private final long end;

    /** @see java.util.Collections#binarySearch(java.util.List, Object, java.util.Comparator) */
    public static int binarySearch(List<Range> list, long offset) {
        int low = 0;
        int high = list.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Range midVal = list.get(mid);
            int cmp = midVal.compareTo(offset);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found
    }

    public Range(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    public long begin() {
        return begin;
    }

    public long end() {
        return end;
    }

    public int compareTo(long value) {
        if (value < begin) return 1;
        if (value > end) return -1;
        return 0;
    }
}
