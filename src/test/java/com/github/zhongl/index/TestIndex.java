package com.github.zhongl.index;

import com.google.common.base.Function;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class TestIndex extends Index {

    private final boolean removed;

    public TestIndex(int i, boolean removed) {
        super(new TestKey(i));
        this.removed = removed;
    }

    @Override
    public boolean isRemoved() { return removed; }

    @Override
    public <I, O> O get(Function<I, O> function) { throw new UnsupportedOperationException(); }

}
