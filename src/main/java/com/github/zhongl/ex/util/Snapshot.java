package com.github.zhongl.ex.util;

import com.github.zhongl.ex.nio.Closable;

import java.io.IOException;
import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Snapshot<I> extends Closable {
    Snapshot merge(Iterator<I> input) throws IOException;

    void remove();
}
