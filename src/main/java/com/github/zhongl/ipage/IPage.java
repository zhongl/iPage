package com.github.zhongl.ipage;

import com.github.zhongl.util.FileNumberNameComparator;
import com.github.zhongl.util.NumberFileNameFilter;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class IPage implements Closeable, Iterable<Record> {

    private final File baseDir;
    private final int chunkCapcity;
    private final List<Chunk> chunks; // TODO use LRU to cache chunks
    private final AbstractList<Range> chunkOffsetRangeList;

    public static Builder baseOn(File dir) {
        return new Builder(dir);
    }

    private IPage(File baseDir, int chunkCapcity, List<Chunk> chunks) {
        this.baseDir = baseDir;
        this.chunkCapcity = chunkCapcity;
        this.chunks = chunks;
        chunkOffsetRangeList = new ChunkOffsetRangeList();
    }

    public long append(Record record) throws IOException {
        try {
            releaseChunkIfNecessary();
            return lastRecentlyUsedChunk().append(record);
        } catch (OverflowException e) {
            grow();
            return append(record);
        }
    }

    private void releaseChunkIfNecessary() {
        // TODO releaseChunkIfNecessary
    }

    private Chunk lastRecentlyUsedChunk() throws IOException {
        if (chunks.isEmpty()) return grow();
        return chunks.get(0);
    }

    private Chunk grow() throws IOException {
        long beginPositionInIPage = chunks.isEmpty() ? 0L : lastRecentlyUsedChunk().endPositionInIPage() + 1;
        Chunk chunk = new Chunk(beginPositionInIPage, new File(baseDir, beginPositionInIPage + ""), chunkCapcity);
        chunks.add(0, chunk);
        return chunk;
    }

    public Record get(long offset) throws IOException {
        if (chunks.isEmpty()) return null;
        releaseChunkIfNecessary();
        return chunkIn(offset).get(offset);
    }

    private Chunk chunkIn(long offset) {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        return chunks.get(index);
    }

    public void flush() throws IOException {
        lastRecentlyUsedChunk().flush();
        releaseChunkIfNecessary();
    }

    @Override
    public void close() throws IOException {
        for (Chunk chunk : chunks) {
            chunk.close();
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return null;  // TODO iterator
    }

    /**
     * Remove {@link Record} before the offset.
     *
     * @param offset
     */
    public void truncate(long offset) throws IOException {
        int index = Range.binarySearch(chunkOffsetRangeList, offset);
        Chunk toTruncateChunk = chunks.get(index);
        List<Chunk> toRmoved = chunks.subList(index + 1, chunks.size());
        for (Chunk chunk : toRmoved) {
            chunk.erase();
        }
        toRmoved.clear();
        chunks.add(toTruncateChunk.truncate(offset));
    }

    public void recover() throws IOException {
        lastRecentlyUsedChunk().recover();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("IPage");
        sb.append("{baseDir=").append(baseDir);
        sb.append(", chunkCapacity=").append(chunkCapcity);
        sb.append(", chunks=").append(chunks);
        sb.append('}');
        return sb.toString();
    }


    public static final class Builder {

        private static final int UNSET = -1;

        private final File baseDir;
        private int chunkCapacity = UNSET;

        public Builder(File dir) {
            if (!dir.exists()) checkState(dir.mkdirs(), "Can not create directory: %s", dir);
            checkArgument(dir.isDirectory(), "%s should be a directory.", dir);
            baseDir = dir;
        }

        public Builder chunkCapacity(int value) {
            checkState(chunkCapacity == UNSET, "Chunk capacity can only set once.");
            checkArgument(value >= Chunk.DEFAULT_CAPACITY, "Chunk capacity should not less than %s", Chunk.DEFAULT_CAPACITY);
            chunkCapacity = value;
            return this;
        }

        public IPage build() throws IOException {
            chunkCapacity = (chunkCapacity == UNSET) ? Chunk.DEFAULT_CAPACITY : chunkCapacity;
            List<Chunk> chunks = validateAndLoadExistChunks();
            return new IPage(baseDir, chunkCapacity, chunks);
        }

        private List<Chunk> validateAndLoadExistChunks() throws IOException {
            File[] files = baseDir.listFiles(new NumberFileNameFilter());
            Arrays.sort(files, new FileNumberNameComparator());

            ArrayList<Chunk> chunks = new ArrayList<Chunk>(files.length);
            for (File file : files) {
                // TODO validateAndRecoverBy chunks
                Chunk chunk = new Chunk(Long.parseLong(file.getName()), file, chunkCapacity);
                chunks.add(0, chunk); // reverse order to make sure the appending chunk at first.
            }
            return chunks;
        }

    }

    private class ChunkOffsetRangeList extends AbstractList<Range> {
        @Override
        public Range get(int index) {
            Chunk chunk = IPage.this.chunks.get(index);
            return new Range(chunk.beginPositionInIPage(), chunk.endPositionInIPage());
        }

        @Override
        public int size() {
            return IPage.this.chunks.size();
        }
    }
}
