package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ParallelEncodeBatchTest extends BatchTest {
    @Override
    protected Batch newBatch(CursorFactory cursorFactory, int position, int estimateBufferSize) {
        return new ParallelEncodeBatch(cursorFactory, position, estimateBufferSize);
    }
}
