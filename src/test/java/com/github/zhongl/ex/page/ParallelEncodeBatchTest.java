package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;

import java.io.File;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ParallelEncodeBatchTest extends BatchTest {
    @Override
    protected Batch newBatch(File file, int position, Codec codec, int estimateBufferSize) {
        return new ParallelEncodeBatch(file, position, codec, estimateBufferSize);
    }
}
