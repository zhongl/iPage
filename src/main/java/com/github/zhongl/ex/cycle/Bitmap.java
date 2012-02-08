package com.github.zhongl.ex.cycle;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Bitmap {

    private static final int WORD_LENGTH = 8;
    private static final int ADDRESS_BITS_PER_WORD = 6;
    private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private static final long WORD_MASK = 0xffffffffffffffffL;

    private final ByteBuffer buffer;

    public Bitmap(ByteBuffer buffer) {
        checkArgument((buffer.capacity() % WORD_LENGTH) == 0, "Buffer capacity should be a multiple of %s.", WORD_LENGTH);
        this.buffer = buffer;
        reset();
    }

    public Bitmap set(int offset, int length) {
        checkArgument(offset >= 0);
        checkArgument(offset + length <= buffer.capacity());
        if (length == 0) return this;
        return setRange(offset, offset + length);
    }

    private Bitmap setRange(int from, int to) {
        int startWordIndex = wordIndex(from);
        int endWordIndex = wordIndex(to - 1);

        long firstWordMask = WORD_MASK << from;
        long lastWordMask = WORD_MASK >>> -to;

        if (startWordIndex == endWordIndex) {
            orAndPut(startWordIndex, firstWordMask & lastWordMask);
        } else {
            orAndPut(startWordIndex, firstWordMask);

            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                put(i, WORD_MASK);

            orAndPut(endWordIndex, lastWordMask);
        }

        return this;
    }

    public int nextClearBit(int from) {
        int u = wordIndex(from);

        long word = ~get(u) & (WORD_MASK << from);

        while (true) {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
            word = ~get(++u);
        }
    }

    public Bitmap reset() {
        buffer.position(0);
        while (buffer.hasRemaining()) {
            buffer.putLong(0L);
        }
        return this;
    }

    private void orAndPut(int index, long mask) {
        int position = index * WORD_LENGTH;
        long previous = buffer.getLong(position);
        buffer.putLong(position, previous | mask);
    }

    private void put(int index, long mask) {
        buffer.putLong(index * WORD_LENGTH, mask);
    }

    private long get(int index) {
        return buffer.getLong(index * WORD_LENGTH);
    }

    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

}
