package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.page.Numbered;

import java.util.Collections;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
abstract class Binder {
    protected final List<Page> pages;

    protected Binder(List<Page> pages) {this.pages = pages;}

    protected Page binarySearch(com.github.zhongl.ex.page.Number number) {
        int i = Collections.binarySearch(pages, new Numbered(number) {});
        i = i < 0 ? -(i + 2) : i;
        return pages.get(i);
    }
}
