package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.page.DefaultBatch;
import com.github.zhongl.ex.util.Tuple;

import java.nio.ByteBuffer;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class CycleBatch extends DefaultBatch {

    private final int blockSize;

    public CycleBatch(Codec codec, long fileLength, int blockSize, int estimateBufferSize) {
        super(codec, fileLength / blockSize, estimateBufferSize);
        this.blockSize = blockSize;
    }

    @Override
    protected Tuple aggregate(Tuple tuple, ByteBuffers.Aggregater aggregater) {
        ByteBuffer buffer = bufferIn(tuple);

        long offset = position;
        int bufferLength = ByteBuffers.lengthOf(buffer);
        int numOfBlocks = bufferLength / blockSize + 1;

        aggregater.concat(buffer, numOfBlocks * blockSize);

        position += numOfBlocks;

        return new Tuple(callbackIn(tuple), offset, numOfBlocks);
    }

}
