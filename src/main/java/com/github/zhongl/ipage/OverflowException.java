package com.github.zhongl.ipage;

/**
 * {@link OverflowException} means there is no enough remains for new item.
 * <p/>
 * <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
public class OverflowException extends IllegalStateException {
    public OverflowException() {
        super();
    }

    public OverflowException(String msg) {
        super(msg);
    }
}