package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public interface CursorFactory {

    Cursor reader(int offset);

    ObjectRef objectRef(Object object);

    Proxy proxy(Cursor intiCursor);

}
