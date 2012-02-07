package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ParallelEncodeBatchTest extends BatchTest {
    @Override
    protected Batch newBatch(Codec codec, int position, int estimateBufferSize) {
        return new ParallelEncodeBatch(codec, position, estimateBufferSize);
    }
}
