package com.github.zhongl.page;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Element<T> {
    private final T value;
    private final Range range;

    public Element(T value, Range range) {
        this.value = checkNotNull(value);
        this.range = checkNotNull(range);
    }

    public T value() {
        return value;
    }

    public Range range() {
        return range;
    }

    @Override
    public String toString() {
        return "Element{value=" + value + ", range=" + range + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Element that = (Element) o;
        return range.equals(that.range) && value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + range.hashCode();
        return result;
    }
}
