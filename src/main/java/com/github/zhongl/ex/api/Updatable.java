package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Asynchronize;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.lang.Entry;

import java.util.concurrent.Future;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@Asynchronize
interface Updatable {
    void update(Future<Revision> future, Entry<Md5Key, byte[]> entry);
}
