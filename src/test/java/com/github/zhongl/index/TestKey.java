package com.github.zhongl.index;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class TestKey implements Key {
    final int value;

    public TestKey(int i) {value = i;}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return value == ((TestKey) o).value;
    }

    @Override
    public int hashCode() { return value; }

    @Override
    public String toString() { return "TestKey{value=" + value + '}'; }

    @Override
    public int compareTo(Key that) {
        checkArgument(that.getClass() == getClass(), "Expect %s but %s", getClass(), that.getClass());
        if (this == that) return 0;
        long thatValue = ((TestKey) that).value;
        return value > thatValue ? 1 : (value == thatValue ? 0 : -1);
    }
}
