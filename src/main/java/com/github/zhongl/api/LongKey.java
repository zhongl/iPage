package com.github.zhongl.api;

import com.github.zhongl.index.Key;

import javax.annotation.concurrent.ThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class LongKey implements Key {
    private final long value;

    public LongKey(long value) {this.value = value;}

    public long value() { return value; }

    @Override
    public String toString() {
        return "LongKey{value=" + value + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return value == ((LongKey) o).value;
    }

    @Override
    public int hashCode() { return (int) (value ^ (value >>> 32)); }

    @Override
    public int compareTo(Key that) {
        checkArgument(that.getClass() == getClass(), "Expect %s but %s", getClass(), that.getClass());
        if (this == that) return 0;
        long thatValue = ((LongKey) that).value;
        return value > thatValue ? 1 : (value == thatValue ? 0 : -1);
    }

}
