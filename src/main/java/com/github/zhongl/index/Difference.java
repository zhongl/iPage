package com.github.zhongl.index;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public class Difference implements Iterable<Index> {

    private final SortedSet<Index> set;

    public Difference(SortedSet<Index> set) {this.set = set;}

    public boolean addAll(Collection<? extends Index> c) {
        return set.addAll(c);
    }

    public void add(Index index) {
        set.add(index);
    }

    @Override
    public Iterator<Index> iterator() {
        return set.iterator();
    }
}
