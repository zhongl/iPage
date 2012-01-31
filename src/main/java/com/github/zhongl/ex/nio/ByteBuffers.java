package com.github.zhongl.ex.nio;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.ByteBuffer.allocate;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ByteBuffers {
    private ByteBuffers() {}

    public static int lengthOf(ByteBuffer buffer) {
        return buffer.limit() - buffer.position();
    }

    public static ByteBuffer aggregate(ByteBuffer aggregated, ByteBuffer more) {
        checkNotNull(aggregated);
        checkArgument(lengthOf(aggregated) > 0);
        checkNotNull(more);

        int lengthOfMore = lengthOf(more);

        while (aggregated.remaining() < lengthOfMore)
            aggregated = allocate(aggregated.capacity() * 2).put(aggregated);

        return aggregated.put(more);
    }

}
