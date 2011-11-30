package com.github.zhongl.util;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class TimerTest {

    @Test
    public void main() throws Exception {
        Timer timer = new Timer(TimeUnit.MILLISECONDS.toNanos(10L));// 10L ms
        int count = 0;
        for (int i = 0; i < 11; i++) {
            Thread.sleep(5L);
            if (timer.timeout()) {
                count++;
                timer.reset();
            }
        }

        assertThat(count, is(5));
    }
}
