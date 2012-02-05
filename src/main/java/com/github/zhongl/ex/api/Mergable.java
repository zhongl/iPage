package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Asynchronize;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.Offset;

import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@Asynchronize
interface Mergable {
    void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator);
}
