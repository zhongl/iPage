package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Codec<T> {
    T decode(ByteBuffer buffer);

    ByteBuffer encode(T object);
}
