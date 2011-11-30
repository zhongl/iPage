package com.github.zhongl.util;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public final class Timer {
    private final long period;
    private long elpase;
    private long lastPoll;

    public Timer(long period) {
        this.period = period;
    }

    public boolean timeout() {
        if (lastPoll == 0) { // first timeout
            lastPoll = System.nanoTime();
            return false;
        }
        long now = System.nanoTime();
        elpase += now - lastPoll;
        lastPoll = now;
        return elpase >= period;
    }

    public void reset() {
        elpase = 0L;
    }
}
