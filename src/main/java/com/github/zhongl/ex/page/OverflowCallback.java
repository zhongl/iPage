package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface OverflowCallback {
    class OverflowThrowing implements OverflowCallback {
        @Override
        public void onOverflow(Group group, boolean force) {
            throw new IllegalStateException("Overflow");
        }
    }

    void onOverflow(Group group, boolean force);
}
