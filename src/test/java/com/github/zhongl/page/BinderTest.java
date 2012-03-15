package com.github.zhongl.page;

import com.github.zhongl.codec.Codec;
import com.github.zhongl.util.FileTestContext;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BinderTest extends FileTestContext {

    private Binder<Integer> binder;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        dir = testDir("usage");

        binder = new Binder<Integer>(dir, Collections.<Page<Integer>>emptyList(), new Codec<Integer>() {
            @Override
            public Integer decode(ByteBuffer buffer) {
                return buffer.getInt();
            }

            @Override
            public ByteBuffer encode(Integer integer) {
                return (ByteBuffer) ByteBuffer.allocate(4).putInt(integer).flip();
            }
        });
    }

    @Test
    public void usage() throws Exception {

        final List<Element<Integer>> collector = new ArrayList<Element<Integer>>();

        binder = binder.append(Arrays.asList(1, 2, 3), new Function<Element<Integer>, Void>() {
            @Override
            public Void apply(@Nullable Element<Integer> element) {
                collector.add(element);
                return null;
            }
        });

        assertThat(collector, is(Arrays.asList(
                element(1, range(0, 4)),
                element(2, range(4, 8)),
                element(3, range(8, 12))
        )));

        assertThat(binder.get(range(0, 4)), is(1));
        assertThat(binder.get(range(4, 8)), is(2));
        assertThat(binder.get(range(8, 12)), is(3));


        binder = binder.append(Arrays.asList(4, 5, 6), new Function<Element<Integer>, Void>() {
            @Override
            public Void apply(Element<Integer> element) { return null; }
        });

        Iterator<Element<Integer>> iterator;
        iterator = binder.iterator();

        assertThat(iterator.next(), is(element(1, range(0, 4))));
        assertThat(iterator.next(), is(element(2, range(4, 8))));
        assertThat(iterator.next(), is(element(3, range(8, 12))));
        assertThat(iterator.next(), is(element(4, range(12, 16))));
        assertThat(iterator.next(), is(element(5, range(16, 20))));
        assertThat(iterator.next(), is(element(6, range(20, 24))));
        assertThat(iterator.hasNext(), is(false));


        collector.clear();

        binder = binder.defrag(
                new Predicate<Element<Integer>>() {
                    @Override
                    public boolean apply(@Nullable Element<Integer> element) {
                        return element.value() > 4;
                    }
                },
                new Function<Element<Integer>, Void>() {
                    @Override
                    public Void apply(@Nullable Element<Integer> element) {
                        collector.add(element);
                        return null;
                    }
                }
        );

        assertThat(collector, is(Arrays.asList(
                element(5, range(0, 4)),
                element(6, range(4, 8))
        )));


        iterator = binder.iterator();

        assertThat(iterator.next(), is(element(5, range(0, 4))));
        assertThat(iterator.next(), is(element(6, range(4, 8))));
        assertThat(iterator.hasNext(), is(false));

    }


    static <T> Element<T> element(T value, Range range) {
        return new Element(value, range);
    }

    static Range range(long from, long to) {
        return new Range(from, to);
    }
}
