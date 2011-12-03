package com.github.zhongl.ipage;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
final class CallByCountOrElapse {

    private final ByCount byCount;
    private final ByElapse byElapse;
    private final Callable<?> callable;

    public CallByCountOrElapse(int count, long elapseMilliseconds, Callable<?> callable) {
        this.callable = callable;
        this.byCount = new ByCount(count);
        this.byElapse = new ByElapse(elapseMilliseconds);
    }

    private void reset() {
        byCount.reset();
        byElapse.reset();
    }

    public Boolean tryCallByCount() throws Exception {
        if (!byCount.tryCall(callable)) return false;
        reset();
        return true;
    }

    public Boolean tryCallByElapse() throws Exception {
        if (!byElapse.tryCall(callable)) return false;
        reset();
        return true;
    }

    final static class ByCount {
        private int currentCount;
        private final int count;

        public ByCount(int count) {
            this.count = count;
        }

        public boolean tryCall(Callable<?> runnable) throws Exception {
            if (++currentCount < count) return false;

            runnable.call();
            return true;
        }

        public void reset() {currentCount = 0;}

    }

    final static class ByElapse {

        private final long elapseMilliseconds;
        private long currentElpaseMilliseconds;
        private long lastTime = System.nanoTime();

        public ByElapse(long elapseMilliseconds) {
            this.elapseMilliseconds = elapseMilliseconds;
        }

        public boolean tryCall(Callable<?> runnable) throws Exception {
            long now = System.nanoTime();
            currentElpaseMilliseconds += TimeUnit.NANOSECONDS.toMillis(now - lastTime);
            lastTime = now;

            if (currentElpaseMilliseconds < elapseMilliseconds) return false;

            runnable.call();
            return true;
        }

        public void reset() { currentElpaseMilliseconds = 0L; }
    }

}
