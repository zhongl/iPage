package com.github.zhongl.ex.page;

import javax.annotation.Nullable;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface NumberCreator {
    long generateBy(@Nullable Page last);
}
