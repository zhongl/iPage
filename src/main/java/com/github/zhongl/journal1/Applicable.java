package com.github.zhongl.journal1;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public interface Applicable<T> {
    /**
     * Apply {@link com.github.zhongl.journal1.Journal} {@link com.github.zhongl.journal1.Record}.
     *
     * @param record
     *
     * @return true means need save applied point.
     */
    boolean apply(Record record);
}
