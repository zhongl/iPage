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

    public static Aggregater aggregater(int initialCapacity) {
        return new Aggregater(initialCapacity);
    }

    public static class Aggregater {
        private volatile ByteBuffer aggregated;

        private Aggregater(int initialCapacity) {
            checkArgument(initialCapacity > 0);
            this.aggregated = ByteBuffer.allocate(initialCapacity);
        }

        public Aggregater concat(ByteBuffer more, int length) {
            checkNotNull(more);
            int previous = aggregated.position();
            while (aggregated.remaining() < length) {
                aggregated.flip();
                aggregated = allocate(aggregated.capacity() * 2).put(aggregated);
            }
            aggregated.put(more);
            aggregated.position(previous + length);
            return this;
        }

        public ByteBuffer get() {
            return (ByteBuffer) aggregated.flip();
        }
    }

}
