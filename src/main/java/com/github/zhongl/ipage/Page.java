package com.github.zhongl.ipage;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class Page extends Numbered {
    protected final File file;

    protected Page(File file, Number number) {
        super(number);
        this.file = file;
    }

    public File file() {return file;}
}
