package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Numbered implements Comparable<Numbered> {
    private final Number number;

    protected Numbered(Number number) {
        this.number = number;
    }

    public Number number() {
        return number;
    }

    @Override
    public int compareTo(Numbered o) {
        return number().compareTo(o.number());
    }
}
