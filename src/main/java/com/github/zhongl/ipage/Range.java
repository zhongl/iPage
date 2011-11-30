package com.github.zhongl.ipage;

import java.util.AbstractList;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Range {

    private final long begin;
    private final long end;

    /** @see java.util.Collections#binarySearch(java.util.List, Object, java.util.Comparator) */
    public static int binarySearch(AbstractList<Range> list, long offset) {
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

    public int compareTo(long value) {
        if (value < begin) return -1;
        if (value > end) return 1;
        return 0;
    }
}
