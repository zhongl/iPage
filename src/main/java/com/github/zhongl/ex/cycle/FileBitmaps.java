package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.nio.Closable;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileBitmaps implements Closable {

    private final int pageCapcity;

    private volatile InnerBinder currentSnapshot;

    public FileBitmaps(File dir, int pageCapcity) throws IOException {
        this.pageCapcity = pageCapcity;

        List<InnerBinder> list = new FilesLoader<InnerBinder>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<InnerBinder>() {
                    @Override
                    public InnerBinder transform(File file, boolean last) throws IOException {
                        return new InnerBinder(file);
                    }
                }
        ).loadTo(new ArrayList<InnerBinder>());

        if (list.isEmpty()) {
            currentSnapshot = new InnerBinder(new File(dir, "0"));
        } else {
            // In this case, it means index merged failed last time because of crash.
            // So the simplest way is keep only the first binder, and remove rest, then wait for recovery.
            currentSnapshot = list.remove(0);
            for (InnerBinder InnerBinder : list) InnerBinder.remove();
        }

    }

    public void merge(Iterator<DefaultCursor> cursorIterator) {
        if (!cursorIterator.hasNext()) return;
        currentSnapshot = currentSnapshot.merge(cursorIterator);
    }

    public long nextSetBit(long from) {
        return currentSnapshot.nextSetBit(from);
    }

    public long nextClearBit(long from) {
        return currentSnapshot.nextClearBit(from);
    }

    @Override
    public void close() {
        currentSnapshot.close();
    }

    private class InnerBinder extends Binder<Object> {

        protected InnerBinder(File dir) throws IOException {
            super(dir, null);
        }

        @Override
        protected com.github.zhongl.ex.page.Number newNumber(@Nullable Page last) {
            if (last == null) return new Offset(0L);
            Offset offset = (Offset) last.number();
            return offset.add(last.file().length() * Bitmap.WORD_LENGTH);
        }

        @Override
        protected com.github.zhongl.ex.page.Number parseNumber(String text) {
            return new Offset(text);
        }

        @Override
        protected Page newPage(File file, com.github.zhongl.ex.page.Number number, Codec codec) {
            return new FileBitmap(file, number, codec);
        }

        public long nextSetBit(long from) {
            int index = binarySearchPageIndex(new Offset(from));
            checkState(index > -1, "Invalid start %s for next set bit.", from);
            FileBitmap bitmap = (FileBitmap) pages.get(index);
            Offset offset = (Offset) bitmap.number();
            return bitmap.nextSetBit((int) (from - offset.value())) + offset.value();
        }

        public long nextClearBit(long from) {
            int index = binarySearchPageIndex(new Offset(from));
            checkState(index > -1, "Invalid start %s for next clear bit.", from);
            FileBitmap bitmap = (FileBitmap) pages.get(index);
            Offset offset = (Offset) bitmap.number();
            return bitmap.nextClearBit((int) (from - offset.value())) + offset.value();
        }

        public InnerBinder merge(Iterator<DefaultCursor> cursorIterator) {
            return null;  // TODO merge
        }

        public void remove() {
            while (!pages.isEmpty()) {
                Page<Object> page = pages.remove(0);
                page.close();
                checkState(page.file().delete());
            }
            checkState(dir.delete());
        }

        private class FileBitmap extends Page<Object> {
            public FileBitmap(File file, com.github.zhongl.ex.page.Number number, Codec codec) {
                super(file, number, codec);
            }

            @Override
            protected boolean isOverflow() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected Batch<Object> newBatch(int estimateBufferSize) {
                throw new UnsupportedOperationException();
            }

            public int nextClearBit(int from) {
                return 0;  // TODO nextClearBit
            }

            public int nextSetBit(int from) {
                return 0;  // TODO nextSetBit
            }
        }
    }
}
