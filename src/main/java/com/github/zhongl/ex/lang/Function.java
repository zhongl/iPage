package com.github.zhongl.ex.lang;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Function<I, O> {
    O apply(I input);
}
