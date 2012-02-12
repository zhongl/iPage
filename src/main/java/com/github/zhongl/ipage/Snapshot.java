package com.github.zhongl.ipage;

import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.github.zhongl.util.Tuple;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Text File Format:
 * <p/>
 * <pre>
 *  TYPE[L, I] NUMBER FILE_NAME \n
 * </pre>
 * <p/>
 * eg:
 * <pre>
 *  L 287463 b8d1b43eae73587ba56baef574709ecb \n
 * </pre>
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
abstract class Snapshot implements Iterable {

    private final List<Tuple> linePageTuples;
    private final List<Tuple> indexPageTuples;

    private final ReadOnlyLine readOnlyLine;
    private final ReadOnlyIndex readOnlyIndex;
    private final File file;

    public Snapshot(final File file) throws IOException {
        this.file = file;
        linePageTuples = new LinkedList<Tuple>();
        indexPageTuples = new LinkedList<Tuple>();

        Files.readLines(file, Charset.defaultCharset(), new LineProcessor<Void>() {
            @Override
            public boolean processLine(String line) throws IOException {
                String[] parts = line.split(" ");
                Type.valueOf(parts[0]).apply(parts[1], parts[2], file.getParentFile(), linePageTuples, indexPageTuples);
                return true;
            }

            @Override
            public Void getResult() {
                return Nils.VOID;
            }
        });

        readOnlyLine = new ReadOnlyLine(linePageTuples) {
            @Override
            protected <T> T decode(ByteBuffer buffer) {
                return Snapshot.this.decode(buffer);
            }

        };
        readOnlyIndex = new ReadOnlyIndex(indexPageTuples);
    }

    public <T> T get(Key key) {
        return readOnlyLine.get(readOnlyIndex.get(key));
    }

    @Override
    public Iterator iterator() {
        final Iterator iterator = readOnlyLine.iterator();
        return new AbstractIterator() {
            @Override
            protected Object computeNext() {
                while (iterator.hasNext()) {
                    Entry<Key, Object> entry = (Entry<Key, Object>) iterator.next();
                    Range range = readOnlyIndex.get(entry.key());
                    if (range != null) return entry.value();
                }
                return endOfData();
            }
        };
    }

    public <T> void merge(Collection<Entry<Key, T>> appendings, Collection<Key> removings, File tmp) {
        if (readOnlyIndex.aliveRadio(-removings.size()) < 0.5)
            defrag(readOnlyLine, readOnlyIndex, appendings, removings, tmp);
        else
            append(readOnlyLine, readOnlyIndex, appendings, removings, tmp);
    }

    protected <T> void append(
            ReadOnlyLine readOnlyLine,
            ReadOnlyIndex readOnlyIndex,
            Collection<Entry<Key, T>> appendings,
            Collection<Key> removings, File tmp
    ) {
        SortedSet<Entry<Key, Range>> entries = new TreeSet<Entry<Key, Range>>();

        LineAppender lineAppender = new LineAppender(tmp, readOnlyLine.length());

        long position = readOnlyLine.length();

        for (Entry<Key, T> entry : appendings) {
            int length = lineAppender.append(encode(entry));
            entries.add(new Entry<Key, Range>(entry.key(), new Range(position, position + length)));
            position += length;
        }

        lineAppender.force();

        for (Key key : removings) entries.add(new Entry<Key, Range>(key, Range.NIL));

        int capacity = readOnlyIndex.entries().size() + appendings.size();

        IndexMerger indexMerger = new IndexMerger(tmp, capacity) {

            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return false;
            }

        };

        indexMerger.merge(readOnlyIndex.entries().iterator(), entries.iterator());
        indexMerger.force();

        createSnapshotFile(indexMerger, lineAppender, true);
    }

    protected <T> void defrag(
            ReadOnlyLine readOnlyLine,
            final ReadOnlyIndex readOnlyIndex,
            Collection<Entry<Key, T>> appendings,
            Collection<Key> removings, File tmp
    ) {
        SortedSet<Entry<Key, Range>> entries = new TreeSet<Entry<Key, Range>>();

        for (Entry<Key, T> entry : appendings) entries.add(new Entry<Key, Range>(entry.key(), Range.NIL));
        for (Key key : removings) entries.add(new Entry<Key, Range>(key, Range.NIL));

        int capacity = readOnlyIndex.entries().size() + appendings.size() - removings.size();
        final IndexMerger indexMerger = new IndexMerger(tmp, capacity) {
            @Override
            protected boolean remove(Entry<Key, Range> c) {
                return c.value() == Range.NIL;
            }

        };

        indexMerger.merge(readOnlyIndex.entries().iterator(), entries.iterator());

        final LineAppender lineAppender = new LineAppender(tmp, 0L);

        Migrater migrater = new Migrater() {
            long position;

            @Override
            public void migrate(Key key, ByteBuffer buffer) {
                if (indexMerger.get(key) == Range.NIL) return;
                int length = lineAppender.append(buffer);
                indexMerger.set(key, new Range(position, position + length));
                position += length;
            }
        };

        readOnlyLine.migrateBy(migrater);

        for (Entry<Key, T> entry : appendings) {
            migrater.migrate(entry.key(), encode(entry));
        }

        indexMerger.force();
        lineAppender.force();

        createSnapshotFile(indexMerger, lineAppender, false);
    }

    private void createSnapshotFile(IndexMerger indexMerger, LineAppender lineAppender, boolean append) {
        // TODO createSnapshotFile
    }

    public void delete() {
        for (Tuple tuple : linePageTuples) tuple.<File>get(1).delete();
        for (Tuple tuple : indexPageTuples) tuple.<File>get(1).delete();
        file.delete();
    }

    protected abstract <T> ByteBuffer encode(Entry<Key, T> entry);

    protected abstract <T> T decode(ByteBuffer buffer);
}
