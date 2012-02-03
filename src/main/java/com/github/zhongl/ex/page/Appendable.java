package com.github.zhongl.ex.page;

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Appendable {
    <T> Cursor append(T value, boolean force) throws IOException;
}
