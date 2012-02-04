package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Writer {

    public abstract void append(Md5Key key, byte[] value) throws IOException;

    public abstract void remove(Md5Key key) throws IOException;

    abstract int thoughtput(int delta);

}
