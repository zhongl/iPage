package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Browser {
    byte[] get(Md5Key key);
}
