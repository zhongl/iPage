package com.github.zhongl.ipage;

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Recovery implements Runnable {

    private final Index index;
    private final IPage ipage;

    public Recovery(Index index, IPage ipage) {
        this.index = index;
        this.ipage = ipage;
    }

    @Override
    public void run() {
        try {
            index.recoverBy(new InnerRecordFinder());
            ipage.recover();
        } catch (IOException e) {
            throw new IllegalStateException("Can't run recovery, because:", e);
        }
    }

    public interface RecordFinder {
        Record getRecordIn(long offset) throws IOException;
    }

    class InnerRecordFinder implements RecordFinder {
        @Override
        public Record getRecordIn(long offset) throws IOException {
            try {
                return ipage.get(offset);
            } catch (IllegalArgumentException e) {
                return null;// offset or length is illegal
            }
        }
    }

}
