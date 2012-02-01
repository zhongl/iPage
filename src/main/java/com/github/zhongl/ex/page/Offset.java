package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Offset extends Number {
    private final Long value;

    public Offset(Long value) {
        this.value = value;
    }

    @Override
    public int compareTo(Number o) {
        return value.compareTo(((Offset) o).value);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    public Offset add(long length) {
        return new Offset(value + length);
    }
}
