package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StringCodec implements Codec<String> {
    @Override
    public String decode(ByteBuffer buffer) {
        int length = buffer.limit() - buffer.position();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

    @Override
    public ByteBuffer encode(String object) {
        return ByteBuffer.wrap(object.getBytes());
    }
}
