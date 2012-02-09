package com.github.zhongl.ex.cycle;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.ex.util.Bitmap;
import com.github.zhongl.ex.util.SnapshotKeeper;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileBitmaps extends SnapshotKeeper<SnapshotBinder, DefaultCursor> {

    private final int pageCapcity;

    public FileBitmaps(File dir, int pageCapcity) throws IOException {
        super(dir, new Factory<SnapshotBinder>() {
            @Override
            public SnapshotBinder create(File file) throws IOException {
                return new InnerBinder(file);
            }
        });
        this.pageCapcity = pageCapcity;
    }

    public long nextSetBit(long from) {
        return ((InnerBinder) currentSnapshot).nextSetBit(from);
    }

    public long nextClearBit(long from) {
        return ((InnerBinder) currentSnapshot).nextClearBit(from);
    }

    static class InnerBinder extends SnapshotBinder<DefaultCursor> {

        protected InnerBinder(File dir) throws IOException {
            super(dir, null);
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            if (last == null) return new Offset(0L);
            Offset offset = (Offset) last.number();
            return offset.add(last.file().length() * Bitmap.WORD_LENGTH);
        }

        @Override
        protected Number parseNumber(String text) {
            return new Offset(text);
        }

        @Override
        protected Page<Object> newPage(File file, Number number, Codec codec) {
            return new FileBitmap(file, number, codec);
        }

        public Long nextSetBit(long from) {
            return doInPage(from, new Fun() {
                @Override
                public int apply(FileBitmap fileBitmap, int offset) {
                    return fileBitmap.nextSetBit(offset);
                }
            });
        }

        private Long doInPage(long offset, Fun fun) {
            int index = binarySearchPageIndex(new Offset(offset));
            checkState(index > -1, "Invalid offset %s for search page.", offset);
            FileBitmap fileBitmap = (FileBitmap) pages.get(index);
            Offset number = (Offset) fileBitmap.number();
            int l = (int) (offset - number.value());
            return fun.apply(fileBitmap, l) + number.value();
        }

        public long nextClearBit(long from) {
            return doInPage(from, new Fun() {
                @Override
                public int apply(FileBitmap fileBitmap, int offset) {
                    return fileBitmap.nextClearBit(offset);
                }
            });
        }

        @Override
        protected InnerBinder next(File dir) throws IOException {
            return new InnerBinder(dir);
        }

        @Override
        protected void merge(Iterator<DefaultCursor> iterator, SnapshotBinder binder) {
            // TODO merge
        }

        @Override
        protected boolean isEmpty() {
            return pages.get(0).file().length() == 0;
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
            public FileBitmap(File file, Number number, Codec codec) {
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

        interface Fun {
            int apply(FileBitmap fileBitmap, int offset);
        }
    }


}
