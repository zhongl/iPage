package com.github.zhongl.index;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class TestIndexCodec implements IndexCodec {
    @Override
    public ByteBuffer encode(Index value) {
        return (ByteBuffer) ByteBuffer.allocate(4).putInt(((TestKey) value.key()).value).flip();
    }

    @Override
    public Index decode(ByteBuffer byteBuffer) { return new TestIndex(byteBuffer.getInt(), false); }

    @Override
    public int length() { return 4; }
}
