package com.github.zhongl.util;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Sync<T> implements FutureCallback<T> {

    private final CountDownLatch latch = new CountDownLatch(1);
    private T result;
    private Throwable t;

    @Override
    public void onSuccess(T result) {
        this.result = result;
        latch.countDown();
    }

    public T get() throws IOException, InterruptedException {
        latch.await();
        Throwables.propagateIfPossible(t, IOException.class); // cast IOException and throw
        if (t != null) Throwables.propagate(t); // cast RuntimeException Or Error and throw
        return result;
    }

    @Override
    public void onFailure(Throwable t) {
        this.t = t;
        latch.countDown();
    }
}
