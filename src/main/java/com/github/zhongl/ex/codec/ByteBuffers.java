package com.github.zhongl.ex.codec;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ByteBuffers {
    private ByteBuffers() {}

    public static int lengthOf(ByteBuffer buffer) {
        return buffer.limit() - buffer.position();
    }
}
