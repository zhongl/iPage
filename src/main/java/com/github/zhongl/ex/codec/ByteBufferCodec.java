package com.github.zhongl.ex.codec;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ByteBufferCodec implements Codec {
    @Override
    public ByteBuffer encode(Object instance) {
        return (ByteBuffer) instance;
    }

    @Override
    public ByteBuffer decode(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        buffer.limit(buffer.limit());
        return duplicate;
    }

    @Override
    public boolean supports(Class<?> type) {
        return ByteBuffer.class.isAssignableFrom(type);
    }

}
