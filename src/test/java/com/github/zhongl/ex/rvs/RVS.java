package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.nio.ReadOnlyMappedBuffers;
import com.github.zhongl.ex.page.Number;
import com.github.zhongl.ex.page.Numbered;
import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RVS {

    public static void main(String[] args) {
        TreeSet<Integer> set = new TreeSet<Integer>();
        set.add(5);
        set.add(3);
        set.add(8);
        set.add(16);

        Collection<Object> collection = Collections2.transform(set, new Function<Integer, Object>() {
            @Override
            public Object apply(@Nullable Integer input) {
                return input.toString();
            }
        });

        System.out.println(collection);
        for (Object o : collection) {
            System.out.println(o);
        }
    }
}

class IPage<K extends Comparable<K>, V> {
    Storage<K, V> storage;

    QuanlityOfService quanlityOfService;

    Ephemerons<K, V> ephemerons = new Ephemerons<K, V>(new ConcurrentHashMap<K, Ephemerons<K, V>.Record>()) {
        @Override
        protected void requestFlush(Collection<Entry<K, V>> appendings, Collection<K> removings, FutureCallback<Void> flushedCallback) {
            storage.merge(appendings, removings, flushedCallback);
        }

        @Override
        protected V getMiss(K key) {
            return storage.get(key);
        }

    };

    /**
     * @param key
     * @param value
     *
     * @throws IllegalStateException cause by {@link com.github.zhongl.ex.rvs.QuanlityOfService#RELIABLE}
     */
    public void add(final K key, final V value) {
        quanlityOfService.call(new Function<FutureCallback<Void>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<Void> removedOrDurableCallback) {
                ephemerons.add(key, value, removedOrDurableCallback);
                return null;
            }
        });
    }

    public void remove(K key) {ephemerons.remove(key);}

    public V get(K key) {return ephemerons.get(key);}

}

enum QuanlityOfService {
    RELIABLE {
        @Override
        public void call(Function<FutureCallback<Void>, Void> function) {
            CallbackFuture<Void> future = new CallbackFuture<Void>();
            function.apply(future);
            FutureCallbacks.getUnchecked(future);
        }
    }, LATENCY {
        @Override
        public void call(Function<FutureCallback<Void>, Void> function) {
            function.apply(FutureCallbacks.<Void>ignore());
        }
    };

    public abstract void call(Function<FutureCallback<Void>, Void> function);
}


class Revision implements Iterable {

    private Array array;
    private Index index;

    public Revision(File dir) {
        // TODO Revision
    }

    public <T> T get(Key key) {
        return array.get(index.get(key));
    }

    @Override
    public Iterator iterator() {
        final Iterator iterator = array.iterator();
        return new AbstractIterator() {
            @Override
            protected Object computeNext() {
                while (iterator.hasNext()) {
                    Entry<Key, Object> entry = (Entry<Key, Object>) iterator.next();
                    Range range = index.get(entry.key());
                    if (range != null) return entry.value();
                }
                return endOfData();
            }
        };
    }

    public <T> void merge(Collection<Entry<Key, T>> appendings, Collection<Key> removings, File tmp) throws IOException {
        int delta = appendings.size() - removings.size();
        if (index.aliveRadio() < 0.5) {
            IndexWriter indexWriter = new IndexWriter(tmp, index.alives() + delta);
            Collection<Entry<Key, Range>> entries = index.entries();

            // do compact
        }

        // TODO merge
    }
}

class IndexWriter {
    public IndexWriter(File dir, int capacity) {
        // TODO IndexWriter
    }
}


abstract class Page extends Numbered {
    protected final File file;

    protected Page(File file, Number number) {
        super(number);
        this.file = file;
    }

    public File file() {return file;}
}

abstract class Binder {
    protected final List<Page> pages;

    protected Binder(List<Page> pages) {this.pages = pages;}

    protected abstract Page binarySearch(Number number);
}

@ThreadSafe
abstract class Array extends Binder implements Iterable {

    protected Array(List<Page> pages) {
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

    protected abstract int refer(long absolute, Page reference);

    protected abstract <T> T decode(ByteBuffer buffer);

    protected abstract Number number(Range range);

}

// [from, to)
class Range {

    private final long from;
    private final long to;

    public Range(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long from() {return from;}

    public long to() {return to;}
}

abstract class Index extends Binder {

    protected Index(List<Page> pages) {
        super(pages);
    }

    public abstract Range get(Key key);

    public abstract double aliveRadio();

    public abstract Collection<Entry<Key, Range>> entries();

    public abstract int alives();

    public abstract int total();
}

abstract class Key implements Comparable<Key> {}


enum Type {
    I("Index"), O("Object"), B("Bitset"), R("Revision");

    private final String text;

    Type(String text) { this.text = text; }

    public String text() { return text; }
}
