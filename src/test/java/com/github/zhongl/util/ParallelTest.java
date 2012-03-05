package com.github.zhongl.util;

import com.google.common.base.Function;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ParallelTest {
    @Test
    public void cpuBoundMap() throws Exception {
        Collection<String> collection = Arrays.asList("1", "2");
        Function<String, Integer> function = new Function<String, Integer>() {
            @Override
            public Integer apply(@Nullable String input) {
                return input.length();
            }
        };
        Collection<Integer> integers = Parallel.map(collection, function, Parallel.cpuBoundExecutor());

        Iterator<Integer> iterator = integers.iterator();
        assertThat(iterator.next(), is(1));
        assertThat(iterator.next(), is(1));
    }
}
