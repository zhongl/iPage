package com.github.zhongl.ex.index;

import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.Batch;
import com.github.zhongl.ex.page.Offset;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index implements Closable {

    public abstract void merge(Batch batch);

    public abstract Offset get(Md5Key key);
}
