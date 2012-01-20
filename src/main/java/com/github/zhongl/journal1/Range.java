package com.github.zhongl.journal1;

import com.google.common.base.Function;

/**
 * Range = [head, tail)
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
abstract class Range implements Comparable<Long> {
    private final long head;
    private final long tail;

    protected Range(long head, long tail) {
        this.head = head;
        this.tail = tail;
    }

    public long head() {return head;}

    public long tail() {return tail;}

    public abstract Record record(long offset);

    public void foreach(Function<Record, Void> function) {
        for (Record record = record(head);
             record != Record.EOF;
             record = record(record.offset() + record.length())) {

            function.apply(record);
        }
    }

    public abstract Range head(long offset);

    public abstract Range tail(long offset);

    public abstract void remove();

    @Override
    public int compareTo(Long offset) {
        if (offset < head) return 1;
        if (offset >= tail) return -1;
        return 0;
    }

}
