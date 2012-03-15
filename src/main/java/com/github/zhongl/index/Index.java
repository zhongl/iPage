package com.github.zhongl.index;

import com.google.common.base.Function;

import static com.google.common.base.Preconditions.checkNotNull;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class Index implements Comparable<Index> {
    private final Key key;

    protected Index(Key key) { this.key = checkNotNull(key); }

    public Key key() { return key; }

    @Override
    public int compareTo(Index o) { return key.compareTo(checkNotNull(o).key()); }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Index index = (Index) o;
        return key.equals(index.key);
    }

    @Override
    public int hashCode() { return key.hashCode(); }

    @Override
    public String toString() { return getClass().getSimpleName() + "{key=" + key + ", removed=" + isRemoved() + '}'; }

    public abstract boolean isRemoved();

    public abstract <Clue, Value> Value get(Function<Clue, Value> function);
}
