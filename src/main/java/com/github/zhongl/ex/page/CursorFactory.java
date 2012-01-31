package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public interface CursorFactory {

    <T> Cursor<T> reader(int offset);

    <T> ObjectRef<T> objectRef(T object);

    <T> Proxy<T> transformer(Cursor<T> intiCursor);

}
