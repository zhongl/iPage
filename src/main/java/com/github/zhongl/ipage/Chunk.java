package com.github.zhongl.ipage;

import com.github.zhongl.util.DirectByteBufferCleaner;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.nio.MappedByteBuffer;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * {@link com.github.zhongl.ipage.Chunk} File structure :
 * <ul>
 * <p/>
 * <li>{@link com.github.zhongl.ipage.Chunk} = {@link Record}* </li>
 * <li>{@link Record} = length:4bytes bytes</li>
 * </ul>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@NotThreadSafe
final class Chunk implements Closeable {

    static final int DEFAULT_CAPACITY = 4096; // 4k
    private final File file;
    private final long capacity;
    private final long beginPositionInIPage;

    private volatile MappedByteBuffer mappedByteBuffer;
    private volatile int writePosition = 0;

    /**
     * @param beginPositionInIPage
     * @param file
     * @param capacity
     *
     * @throws java.io.IOException
     */
    Chunk(long beginPositionInIPage, File file, long capacity) throws IOException {
        this.beginPositionInIPage = beginPositionInIPage;
        this.file = file;
        this.capacity = capacity;
        this.writePosition = (int) file.length();
    }

    public long append(Record record) throws IOException {
        checkOverFlowIfAppend(record.length());
        long iPageOffset = writePosition + beginPositionInIPage;
        ensureMap();
        mappedByteBuffer.position(writePosition);
        writePosition += record.writeTo(mappedByteBuffer.duplicate());
        return iPageOffset;
    }

    public Record get(long offset) throws IOException {
        ensureMap();
        mappedByteBuffer.position((int) (offset - beginPositionInIPage));
        return Record.readFrom(mappedByteBuffer.duplicate()); // duplicate to avoid modification of mappedDirectBuffer .
    }

    @Override
    public void close() throws IOException {
        if (mappedByteBuffer != null) {
            flush();
            DirectByteBufferCleaner.clean(mappedByteBuffer);
            mappedByteBuffer = null;
            trim();
        }
    }

    public long endPositionInIPage() {
        return beginPositionInIPage + writePosition - 1;
    }

    public long beginPositionInIPage() {
        return beginPositionInIPage;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Chunk");
        sb.append("{file=").append(file);
        sb.append(", capacity=").append(capacity);
        sb.append(", beginPositionInIPage=").append(beginPositionInIPage);
        sb.append(", writePosition=").append(writePosition);
        sb.append('}');
        return sb.toString();
    }

    public void flush() throws IOException {
        if (mappedByteBuffer == null) return;
        mappedByteBuffer.force();
    }

    public Iterator<Record> iterator() {
        return null;  // TODO iterator
    }

    public void erase() throws IOException {
        close();
        checkState(file.delete(), "Can't delete file %s ", file);
    }

    public Chunk truncate(long offset) throws IOException {
        long length = endPositionInIPage() - offset;
        File remains = new File(file.getParentFile(), offset + "");
        InputSupplier<InputStream> source = ByteStreams.slice(Files.newInputStreamSupplier(file), offset, length);
        Files.copy(source, remains);
        erase();
        return new Chunk(offset, remains, length);
    }

    private void checkOverFlowIfAppend(int length) {
        int appendedPosition = writePosition + length + Record.LENGTH_BYTES;
        if (appendedPosition > capacity) throw new OverflowException();
    }

    private void ensureMap() throws IOException {
        // TODO maybe a closed state is needed for prevent remap
        if (mappedByteBuffer == null) mappedByteBuffer = Files.map(file, READ_WRITE, capacity);
    }

    /**
     * Trim for keeping write position when closed.
     *
     * @throws java.io.IOException
     */
    private void trim() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.setLength(writePosition);
        randomAccessFile.close();
    }
}
