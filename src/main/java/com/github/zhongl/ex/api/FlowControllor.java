package com.github.zhongl.ex.api;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FlowControllor {
    private final Semaphore semaphore = new Semaphore(0, true);
    private final AtomicInteger timeout = new AtomicInteger(100);

    /** @return available throughout. */
    public int throughout(int delta) {
        if (delta < 0) semaphore.acquireUninterruptibly(delta);
        if (delta > 0) semaphore.release(delta);
        return semaphore.availablePermits();
    }

    /** @return last timeout milliseconds. */
    public int timeout(int milliseconds) {
        return timeout.getAndSet(milliseconds);
    }

    public <T> T call(Callable<T> callable) throws Exception {
        boolean acquired = semaphore.tryAcquire(timeout.get(), MILLISECONDS);
        if (!acquired) throw new InterruptedException();
        try {
            return callable.call();
        } finally {
            semaphore.release();
        }
    }
}
