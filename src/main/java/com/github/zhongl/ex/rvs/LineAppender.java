package com.github.zhongl.ex.rvs;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class LineAppender {
    public abstract int append(ByteBuffer buffer);

    public abstract void force();
}
