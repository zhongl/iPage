package com.github.zhongl.ex.index;

import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.Offset;

import java.util.Iterator;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index implements Closable {

    public abstract void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator);

    public abstract Offset get(Md5Key key);
}
