package com.github.zhongl.ex.util;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Tuple {
    private final Object[] objects;

    public Tuple(Object... objects) {
        this.objects = objects;
    }

    public <T> T get(int index) {
        return (T) objects[index];
    }
}
