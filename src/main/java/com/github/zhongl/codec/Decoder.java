package com.github.zhongl.codec;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Decoder<T> {
    T decode(ByteBuffer byteBuffer);
}
