package com.github.zhongl.codec;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StringCodec implements Codec {

    @Override
    public ByteBuffer encode(Object instance) {
        return ByteBuffer.wrap(((String) instance).getBytes());
    }

    @Override
    public String decode(ByteBuffer buffer) {
        int length = ByteBuffers.lengthOf(buffer);
        if (buffer.isDirect()) {
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return new String(bytes);
        } else {
            byte[] bytes = buffer.array();
            int offset = buffer.position();
            buffer.position(offset + length);
            return new String(bytes, offset, length);
        }
    }

    @Override
    public boolean supports(Class<?> type) {
        return String.class.equals(type);
    }
}
