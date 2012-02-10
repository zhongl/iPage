package com.github.zhongl.ex.blocks;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.ByteBuffers;
import com.github.zhongl.ex.nio.FileChannels;
import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.ex.util.Tuple;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Blocks<T> extends Binder<T> {

    public static final int PAGE_CAPACITY = Integer.getInteger("ipage.blocks.page.capacity", (1 << 20) * 64);

    private final BlockSize blockSize;

    protected Blocks(File dir, Codec codec, BlockSize blockSize) throws IOException {
        super(dir, codec);
        this.blockSize = blockSize;
    }

    @Override
    protected Number newNumber(@Nullable Page last) {
        if (last == null) return new Offset(0L);
        Offset offset = (Offset) last.number();
        return offset.add(last.file().length() / blockSize.value());
    }

    @Override
    protected Number parseNumber(String text) {
        return new Offset(text);
    }

    @Override
    protected Page<T> newPage(File file, Number number, Codec codec) {
        return new Page<T>(file, number, codec) {
            @Override
            protected boolean isOverflow() {
                return file().length() > PAGE_CAPACITY;
            }

            @Override
            protected Batch<T> newBatch(int estimateBufferSize) {
                return new DefaultBatch(codec(), file().length() / blockSize.value(), estimateBufferSize) {

                    @Override
                    protected Tuple aggregate(Tuple tuple, ByteBuffers.Aggregater aggregater) {
                        ByteBuffer buffer = bufferIn(tuple);

                        long offset = position;
                        int bufferLength = ByteBuffers.lengthOf(buffer);
                        int numOfBlocks = bufferLength / blockSize.value() + 1;

                        aggregater.concat(buffer, numOfBlocks * blockSize.value());

                        position += numOfBlocks;

                        return new Tuple(callbackIn(tuple), offset, numOfBlocks);
                    }
                };
            }
        };
    }

    public T get(Cursor cursor) throws IOException {
        DefaultCursor defaultCursor = (DefaultCursor) cursor;
        int index = binarySearchPageIndex(new Offset(defaultCursor.offset()));
        checkState(index > -1, "Invalid cursor %s for getting from blocks", defaultCursor.offset());

        Page<T> page = pages.get(index);
        File file = page.file();
        Offset number = (Offset) page.number();
        long offset = defaultCursor.offset() - number.value();
        int length = defaultCursor.length() * blockSize.value();

        if (index < pages.size() - 1) {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(file);
            buffer.limit((int) (offset + length)).position((int) offset);
            return codec.decode(buffer);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(length);
            FileChannel channel = FileChannels.getOrOpen(file);
            channel.position(offset);
            channel.read(buffer);
            return codec.decode(buffer);
        }
    }
}