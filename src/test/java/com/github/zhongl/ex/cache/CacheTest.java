package com.github.zhongl.ex.cache;

import com.google.common.base.Stopwatch;
import org.junit.Test;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CacheTest {
    @Test
    public void main() throws Exception {
        Cache<byte[], byte[]> cache = new Cache<byte[], byte[]>();
        // cache.merge(pages)
        // page = page.merge(sortedMap)
    }

    @Test
    public void skipList() throws Exception {
        SortedMap<Integer, Integer> map = new ConcurrentSkipListMap<Integer, Integer>();

        map.put(1, 1);
        map.put(5, 1);
        map.put(8, 1);
        map.put(12, 1);
        map.put(36, 1);

        Stopwatch stopwatch = new Stopwatch().start();
        SortedMap<Integer, Integer> sub = map.subMap(-1, 5777);
        stopwatch.stop();
//        System.out.println(sub);
        System.out.println(stopwatch.toString());
    }

}
