package com.github.zhongl.ipage;

import com.github.zhongl.util.Sync;
import org.hamcrest.Matcher;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class AssertFutureCallback<T> extends Sync<T> {


    public void assertResult(Matcher<T> matcher) throws IOException, InterruptedException {
        matcher.matches(get());
    }

    public void awaitForDone() throws IOException, InterruptedException {
        assertResult((Matcher<T>) is(notNullValue()));
    }
}
