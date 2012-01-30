package com.github.zhongl.ex.codec;

import com.github.zhongl.ex.nio.ByteBuffers;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class LengthCodec extends DecoratedCodec {

    // TODO use varint
    public static final int LENGTH_LENGTH = 4;

    public LengthCodec(Codec codec) {
        super(codec);
    }

    public ByteBuffer encode(Object instance) {
        ByteBuffer body = delegate.encode(instance);
        int length = ByteBuffers.lengthOf(body);
        ByteBuffer encoded = ByteBuffer.allocate(LENGTH_LENGTH + length);
        encoded.putInt(length).put(body).flip();
        return encoded;
    }

    @Override
    public <T> T decode(ByteBuffer buffer) {
        int length = buffer.getInt();
        ByteBuffer body = buffer.duplicate();
        body.limit(buffer.position() + length);
        buffer.position(body.limit());
        return delegate.decode(body);
    }

}
