package com.github.zhongl.ex.journal;

import com.github.zhongl.ex.page.Number;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Revision extends Number {

    private final Long value;

    public Revision(long value) {
        this.value = value;
    }

    public Revision increment() {
        return new Revision(value + 1);
    }

    @Override
    public int compareTo(Number o) {
        return value.compareTo(((Revision) o).value);
    }

    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
