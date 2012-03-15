package com.github.zhongl.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Range {
    private final long from;
    private final long to;

    public Range(long from, long to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range that = (Range) o;
        return from == that.from && to == that.to;
    }

    @Override
    public int hashCode() {
        int result = (int) (from ^ (from >>> 32));
        result = 31 * result + (int) (to ^ (to >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Range{from=" + from + ", to=" + to + '}';
    }

    public long from() {
        return from;
    }

    public long to() {
        return to;
    }
}
