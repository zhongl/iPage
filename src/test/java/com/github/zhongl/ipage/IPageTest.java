package com.github.zhongl.ipage;

import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.Md5;
import org.junit.Test;

import static com.github.zhongl.ipage.QuanlityOfService.LATENCY;
import static com.github.zhongl.ipage.QuanlityOfService.RELIABLE;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageTest extends FileTestContext {

    @Test
    public void latency() throws Exception {
        dir = testDir("latency");

        long flushMillis = 1000L;
        int flushCount = 1000;
        IPage<String, String> iPage;
        int ephemeronThroughout = 1;
        iPage = new IPage<String, String>(dir, LATENCY, new StringCodec(), ephemeronThroughout, flushMillis, flushCount) {

            @Override
            protected Key transform(String key) {
                return new Key(Md5.md5(key.getBytes()));
            }
        };

        String key = "key";
        String value = "value";
        iPage.add(key, value);
        assertThat(iPage.get(key), is(value));
        iPage.remove(key);
        assertThat(iPage.get(key), is(nullValue()));
        iPage.stop();
    }

    @Test
    public void reliable() throws Exception {
        dir = testDir("reliable");

        IPage<String, String> iPage;
        long flushMillis = 10L;
        int flushCount = 1;
        int ephemeronThroughout = 1;

        iPage = new IPage<String, String>(dir, RELIABLE, new StringCodec(), ephemeronThroughout, flushMillis, flushCount) {
            @Override
            protected Key transform(String key) {
                return new Key(Md5.md5(key.getBytes()));
            }
        };

        String key = "key";
        String value = "value";
        iPage.add(key, value);
        assertThat(iPage.get(key), is(value));
        iPage.remove(key);
        assertThat(iPage.get(key), is(nullValue()));
        iPage.stop();
    }
}
