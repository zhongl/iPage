package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.Number;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
abstract class ReadOnlyLine extends Binder implements Iterable {

    protected ReadOnlyLine(List<Page> pages) {
        super(pages);
    }

    public <T> T get(Range range) {
        return decode(bufferIn(range, binarySearch(number(range))));
    }

    private ByteBuffer bufferIn(Range range, Page page) {
        return (ByteBuffer) ReadOnlyMappedBuffers.getOrMap(page.file())
                                                 .limit(refer(range.to(), page))
                                                 .position(refer(range.from(), page));
    }

    public void migrateBy(Migrater migrater) {
        for (Page page : pages) {
            ByteBuffer buffer = ReadOnlyMappedBuffers.getOrMap(page.file());
            byte[] bytes = new byte[16];
            while (buffer.hasRemaining()) {
                buffer.get(bytes);
                int length = buffer.getInt();
                int position = buffer.position();
                Key key = new Key(bytes);
                ByteBuffer slice = (ByteBuffer) buffer.duplicate()
                                                      .limit(position + length)
                                                      .position(position - 20);
                migrater.migrate(key, slice);
                buffer.position(position + length);
            }
        }
    }

    protected abstract int refer(long absolute, Page reference);

    protected abstract <T> T decode(ByteBuffer buffer);

    protected abstract Number number(Range range);

    public abstract long length();
}
