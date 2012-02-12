package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.Nils;
import com.github.zhongl.ex.util.Tuple;
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
class Snapshot implements Iterable {

    private ReadOnlyLine readOnlyLine;
    private ReadOnlyIndex readOnlyIndex;

    public Snapshot(final File file) throws IOException {
        final List<Tuple> linePageTuples = new LinkedList<Tuple>();
        final List<Tuple> indexPageTuples = new LinkedList<Tuple>();

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

        LineAppender lineAppender = null;

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

    protected <T> void defrag(ReadOnlyLine readOnlyLine, final ReadOnlyIndex readOnlyIndex, Collection<Entry<Key, T>> appendings, Collection<Key> removings, File tmp) {
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

        final LineAppender lineAppender = null;


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

    private <T> ByteBuffer encode(Entry<Key, T> entry) {
        return null;  // TODO encode
    }


    public void delete() {
        // TODO delete
    }
}
