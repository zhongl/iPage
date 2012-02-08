package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ConcentricCycles extends Binder<Object> {
    private final int blockSize;
    private final int pageCapacityOfBlocks;

    public ConcentricCycles(File dir, Codec codec, int blockSize, int pageCapacityOfBlocks) throws IOException {
        super(dir, codec);
        this.blockSize = blockSize;
        this.pageCapacityOfBlocks = pageCapacityOfBlocks;
    }

    @Override
    protected Number newNumber(@Nullable Page last) {
        if (last == null) return new Offset(0L);
        Offset offset = (Offset) last.number();
        return offset.add(last.file().length() / blockSize);
    }

    @Override
    protected Number parseNumber(String text) {
        return new Offset(text);
    }

    @Override
    protected Page newPage(File file, Number number, Codec codec) {
        return new Page(file, number, codec) {
            @Override
            protected boolean isOverflow() {
                Offset offset = (Offset) number();
                boolean overflow = file().length() >= offset.value() * 2 * blockSize;
                Page<Object> last = pages.get(pages.size() - 1);
                if (overflow) migrateUnremovedTo(newPage(last));
                return true;
            }

            private void migrateUnremovedTo(Page<?> last) {
                // todo
            }

            @Override
            protected Batch newBatch(int estimateBufferSize) {
                return new CycleBatch(codec(), file().length(), blockSize, estimateBufferSize);
            }
        };
    }

}
