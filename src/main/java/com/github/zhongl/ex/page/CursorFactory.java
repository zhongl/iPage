package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface CursorFactory {
    <T> Cursor<T> create(int offset);
}
