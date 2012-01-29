package com.github.zhongl.ex.journal;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface OverflowCallback {
    class OverflowThrowing implements OverflowCallback {
        @Override
        public int onOverflow(ByteBuffer rest, boolean force) {
            throw new IllegalStateException("Overflow");
        }
    }

    int onOverflow(ByteBuffer rest, boolean force);
}
