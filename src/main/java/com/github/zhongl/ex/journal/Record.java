package com.github.zhongl.ex.journal;

import javax.annotation.concurrent.ThreadSafe;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Record {
    private final long revision;

    private final Object event;

    public Record(long revision, Object event) {
        this.revision = revision;
        this.event = event;
    }

    public long revision() {
        return revision;
    }

    public Object event() {
        return event;
    }
}
