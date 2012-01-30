package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
interface FetcherFactory {
   <T> Fetcher<T> createBy(long position);
}
