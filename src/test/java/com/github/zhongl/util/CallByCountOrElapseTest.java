package com.github.zhongl.util;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CallByCountOrElapseTest {

    private int count = 3;
    private long elapseMilliseconds = 100L;
    private CallByCountOrElapse callByCountOrElapse;

    @Before
    public void setUp() throws Exception {
        callByCountOrElapse = new CallByCountOrElapse(count, elapseMilliseconds, new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                return null;
            }
        });
    }

    @Test
    public void runByElapseFirst() throws Exception {
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
        Thread.sleep(elapseMilliseconds);
        assertThat(callByCountOrElapse.tryCallByElapse(), is(true)); // run and reset
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));

    }

    @Test
    public void runByCountFirst() throws Exception {
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByCount(), is(false));
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
        Thread.sleep(elapseMilliseconds / 2);
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
        assertThat(callByCountOrElapse.tryCallByCount(), is(true));  // run and reset
        Thread.sleep(elapseMilliseconds / 2);
        assertThat(callByCountOrElapse.tryCallByElapse(), is(false));
    }
}
