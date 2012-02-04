package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Recorder {

    public abstract void append(Md5Key key, byte[] value) throws IOException;

    public abstract void remove(Md5Key key) throws IOException;

    abstract void thoughtput(int delta);

    abstract void applied(Revision revision);

}
