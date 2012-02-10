package com.github.zhongl.ex.blocks;

import com.github.zhongl.ex.page.Cursor;
import com.github.zhongl.ex.util.Snapshot;
import com.github.zhongl.ex.util.SnapshotKeeper;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FileBitmap extends SnapshotKeeper<FileBitmap.Bitmap> {
    protected FileBitmap(File file, final Factory factory) throws IOException {
        super(file, new Factory<FileBitmap.Bitmap>() {
            @Override
            public Bitmap create(File file) throws IOException {
                return new Bitmap(file);  // TODO create
            }
        });
    }

    public void merge(List<Cursor> removingCursors, List<Cursor> appendingCursors) {
        // TODO merge
    }

    static class Bitmap extends com.github.zhongl.ex.util.Bitmap implements Snapshot{

        public Bitmap(File file) {
            // TODO Bitmap
        }

        @Override
        protected ByteBuffer buffer() {
            return null;  // TODO buffer
        }

        @Override
        protected int capacity() {
            return 0;  // TODO capacity
        }

        public Bitmap merge(Iterator<Cursor> curors) throws IOException {

            return null;  // TODO merge
        }

        @Override
        public void remove() {
            // TODO remove
        }

        @Override
        public void close() {
            // TODO close
        }
    }
}
