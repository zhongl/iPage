package com.github.zhongl.journal1;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
interface Range extends Iterable<Record>, Comparable<Long> {
    Record tail();

    Record head();

    Record record(long offset);
}
