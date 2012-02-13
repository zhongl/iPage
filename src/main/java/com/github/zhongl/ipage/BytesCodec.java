package com.github.zhongl.ipage;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BytesCodec implements Codec<byte[]> {
    @Override
    public byte[] decode(ByteBuffer buffer) {
        int length = buffer.limit() - buffer.position();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    @Override
    public ByteBuffer encode(byte[] object) {
        return ByteBuffer.wrap(object);
    }
}
