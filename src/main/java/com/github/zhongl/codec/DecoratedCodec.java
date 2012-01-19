package com.github.zhongl.codec;

import com.google.common.base.Preconditions;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class DecoratedCodec implements Codec {
    protected final Codec delegate;

    public DecoratedCodec(Codec delegate) {
        Preconditions.checkNotNull(delegate);
        this.delegate = delegate;
    }

    public Codec getDelegate() {
        return delegate;
    }

    @Override
    public final boolean supports(Class<?> type) {
        return delegate.supports(type);
    }
}
