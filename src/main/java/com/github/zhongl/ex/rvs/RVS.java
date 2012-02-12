package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.CallbackFuture;
import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RVS {

    public static void main(String[] args) {
        LinkedList<Integer> primes = primeNumbersUnder(2097152);
        System.out.println(primes.size());
        System.out.println(primes.getLast());
    }

    private static LinkedList<Integer> primeNumbersUnder(int i) {
        LinkedList<Integer> numbers = new LinkedList<Integer>();
        for (int j = 2; j < i; j++) {
            if (isPrime(j, numbers)) numbers.add(j);
        }
        return numbers;
    }

    private static boolean isPrime(int i, List<Integer> numbers) {
        for (int number : numbers) {
            if (i % number == 0) return false;
        }
        return true;
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


// [from, to)
class Range {

    public static final Range NIL = new Range(-1L, 0L);

    private final long from;
    private final long to;

    public Range(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long from() {return from;}

    public long to() {return to;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return from == range.from && to == range.to;
    }

    @Override
    public int hashCode() {
        int result = (int) (from ^ (from >>> 32));
        result = 31 * result + (int) (to ^ (to >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Range{" +
                "from=" + from +
                ", to=" + to +
                '}';
    }
}

