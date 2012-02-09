package com.github.zhongl.ex.util;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Bitmap {

    public static final int WORD_LENGTH = 8;

    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private static final long WORD_MASK = 0xffffffffffffffffL;

    protected Bitmap() {
        checkState((capacity() % WORD_LENGTH) == 0, "Buffer capacity should be a multiple of %s.", WORD_LENGTH);
        reset();
    }

    public Bitmap set(int offset, int length) {
        checkArgument(offset >= 0);
        checkArgument(offset + length <= capacity());
        if (length == 0) return this;
        return setRange(offset, offset + length);
    }

    public int nextClearBit(int from) {
        checkArgument(from > -1);
        int u = wordIndex(from);

        ByteBuffer buffer = buffer();

        long word = ~get(buffer, u) & (WORD_MASK << from);

        while (true) {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            word = ~get(buffer, ++u);
        }
    }

    public int nextSetBit(int from) {
        checkArgument(from > -1);

        int u = wordIndex(from);

        ByteBuffer buffer = buffer();

        int boundary = capacity() / WORD_LENGTH;
        if (u >= boundary) return -1;

        long word = get(buffer, u) & (WORD_MASK << from);

        while (true) {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            if (++u >= boundary)
                return -1;
            word = get(buffer, u);
        }
    }

    public Bitmap reset() {
        ByteBuffer buffer = buffer();
        buffer.position(0);
        while (buffer.hasRemaining()) {
            buffer.putLong(0L);
        }
        return this;
    }

    protected abstract ByteBuffer buffer();

    protected abstract int capacity();

    private void orAndPut(ByteBuffer buffer, int index, long mask) {
        int position = index * WORD_LENGTH;
        long previous = buffer.getLong(position);
        buffer.putLong(position, previous | mask);
    }

    private void put(ByteBuffer buffer, int index, long mask) {
        buffer.putLong(index * WORD_LENGTH, mask);
    }

    private long get(ByteBuffer buffer, int index) {
        return buffer.getLong(index * WORD_LENGTH);
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

    private Bitmap setRange(int from, int to) {
        int startWordIndex = wordIndex(from);
        int endWordIndex = wordIndex(to - 1);

        long firstWordMask = WORD_MASK << from;
        long lastWordMask = WORD_MASK >>> -to;

        ByteBuffer buffer = buffer();

        if (startWordIndex == endWordIndex) {
            orAndPut(buffer, startWordIndex, firstWordMask & lastWordMask);
        } else {
            orAndPut(buffer, startWordIndex, firstWordMask);

            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                put(buffer, i, WORD_MASK);

            orAndPut(buffer, endWordIndex, lastWordMask);
        }

        return this;
    }
}
