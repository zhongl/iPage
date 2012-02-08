package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.page.DefaultBatch;
import com.github.zhongl.ex.util.Tuple;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocate;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class CycleBatch extends DefaultBatch {

    private final int blockSize;

    public CycleBatch(Codec codec, long fileLength, int blockSize, int estimateBufferSize) {
        super(codec, fileLength / blockSize, estimateBufferSize);
        this.blockSize = blockSize;
    }

    @Override
    protected Tuple aggregate(Tuple tuple, ByteBuffer aggregated) {
        ByteBuffer buffer = bufferIn(tuple);

        long offset = position;
        int bufferLength = ByteBuffers.lengthOf(buffer);
        int numOfBlocks = bufferLength / blockSize + 1;
        int alignLength = numOfBlocks * blockSize;

        while (aggregated.remaining() < alignLength) {
            aggregated.flip();
            aggregated = allocate(aggregated.capacity() * 2).put(aggregated);
        }

        aggregated.put(buffer);

        aggregated.position(aggregated.position() + (alignLength - bufferLength)); // set align position

        position += numOfBlocks;

        return new Tuple(callbackIn(tuple), offset, numOfBlocks);

    }
}
