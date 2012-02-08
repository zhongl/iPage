package com.github.zhongl.ex.cycle;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Bitmap {

    /*
    * BitSets are packed into arrays of "words."  Currently a word is
    * a long, which consists of 64 bits, requiring 6 address bits.
    * The choice of word size is determined purely by performance concerns.
    */
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
    private final static int BIT_INDEX_MASK = BITS_PER_WORD - 1;

    /* Used to shift left or right for a partial word mask */
    private static final long WORD_MASK = 0xffffffffffffffffL;
    public static final int WORD_LENGTH = 8;

    private final ByteBuffer buffer;

    public Bitmap(int bits) {
        checkArgument(bits >= WORD_LENGTH, "Size should not less than %s.", WORD_LENGTH);
        buffer = ByteBuffer.allocate((wordIndex(bits - 1) + 1) * WORD_LENGTH);
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
//        expandTo(endWordIndex);

        long firstWordMask = WORD_MASK << from;
        long lastWordMask = WORD_MASK >>> -to;

        if (startWordIndex == endWordIndex) {
            // Case 1: One word
            orAndPut(startWordIndex, firstWordMask & lastWordMask);
        } else {
            // Case 2: Multiple words
            // Handle first word
            orAndPut(startWordIndex, firstWordMask);

            // Handle intermediate words, if any
            for (int i = startWordIndex + 1; i < endWordIndex; i++)
                put(i, WORD_MASK);

            // Handle last word (restores invariants)
            orAndPut(endWordIndex, lastWordMask);
        }


        return this;  // TODO setRange
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

    public int nextClearBit(int from) {
        int u = wordIndex(from);
//        if (u >= wordsInUse)
//            return from;

        long word = ~get(u) & (WORD_MASK << from);

        while (true) {
            if (word != 0)
                return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
//            if (++u == wordsInUse)
//                return wordsInUse * BITS_PER_WORD;
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

    /** Given a bit index, return word index containing it. */
    private static int wordIndex(int bitIndex) {
        return bitIndex >> ADDRESS_BITS_PER_WORD;
    }

}
