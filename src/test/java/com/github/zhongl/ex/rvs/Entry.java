package com.github.zhongl.ex.rvs;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Entry<K, V> {

    private final K key;
    private final V value;

    public Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }
}
