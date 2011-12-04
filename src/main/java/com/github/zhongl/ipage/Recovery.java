package com.github.zhongl.ipage;

import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Recovery implements Runnable {

    private final Index index;
    private final IPage ipage;
    private final IndexRecoverer indexRecoverer;
    private final IPageRecoverer iPageRecoverer;

    public Recovery(Index index, IPage ipage) {
        this.index = index;
        this.ipage = ipage;
        indexRecoverer = new InnerIndexRecoverer();
        iPageRecoverer = new InnerIPageRecoverer();
    }

    @Override
    public void run() {
        try {
            index.recoverBy(indexRecoverer);
            ipage.recoverBy(iPageRecoverer);
        } catch (IOException e) {
            throw new IllegalStateException("Can't run recovery, because:", e);
        }
    }

    public interface IndexRecoverer {
        Record getRecordIn(long offset) throws IOException;
    }

    public interface IPageRecoverer {}

    private class InnerIndexRecoverer implements IndexRecoverer {
        @Override
        public Record getRecordIn(long offset) throws IOException {
            return ipage.get(offset);
        }
    }

    private class InnerIPageRecoverer implements IPageRecoverer {}
}
