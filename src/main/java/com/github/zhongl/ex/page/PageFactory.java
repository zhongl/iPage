package com.github.zhongl.ex.page;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface PageFactory {
    Page createOn(File file, long number);
}
