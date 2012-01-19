package com.github.zhongl.journal1;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Record {
    long number();

    <T> T content();
}
