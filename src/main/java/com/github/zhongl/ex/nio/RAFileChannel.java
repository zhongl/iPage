package com.github.zhongl.ex.nio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class RAFileChannel extends FileChannel {
    private final RandomAccessFile file;

    public RAFileChannel(File file) throws FileNotFoundException {
        this.file = new RandomAccessFile(file, "rw");
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return file.getChannel().read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return file.getChannel().read(dsts, offset, length);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return file.getChannel().write(src);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return file.getChannel().write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
        return file.getChannel().position();
    }

    @Override
    public FileChannel position(long newPosition) throws IOException {
        return file.getChannel().position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return file.getChannel().size();
    }

    @Override
    public FileChannel truncate(long size) throws IOException {
        return file.getChannel().truncate(size);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        file.getChannel().force(metaData);
    }

    @Override
    public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
        return file.getChannel().transferTo(position, count, target);
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        return file.getChannel().transferFrom(src, position, count);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return file.getChannel().read(dst, position);
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        return file.getChannel().write(src, position);
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
        return file.getChannel().map(mode, position, size);
    }

    @Override
    public FileLock lock(long position, long size, boolean shared) throws IOException {
        return file.getChannel().lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(long position, long size, boolean shared) throws IOException {
        return file.getChannel().tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
        file.close();
    }
}
