package com.github.zhongl.ex.page;

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface OverflowCallback<T> {
    OverflowCallback THROW_BY_OVERFLOW = new OverflowThrowing();

    class OverflowThrowing<T> implements OverflowCallback<T> {
        @Override
        public Cursor<T> onOverflow(Object value, boolean force) throws IOException{
            throw new IllegalStateException("Oops, value is bigger than page capacity.");
        }
    }

    Cursor<T> onOverflow(T value, boolean force) throws IOException;
}
