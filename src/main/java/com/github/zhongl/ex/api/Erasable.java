package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Asynchronize;
import com.github.zhongl.ex.journal.Revision;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@Asynchronize
interface Erasable {
    void eraseTo(Revision revision);
}
