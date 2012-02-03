package com.github.zhongl.util;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

import static java.text.MessageFormat.format;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Benchmarks {
    private Benchmarks() { }

    public static void benchmark(String name, Runnable runnable, int times) {
        Stopwatch stopwatch = new Stopwatch().start();
        runnable.run();
        stopwatch.stop();

        long latency = stopwatch.elapsedTime(TimeUnit.NANOSECONDS) / times;
        double tps = times * 1.0 / stopwatch.elapsedTime(TimeUnit.MILLISECONDS) * 1000;

        System.out.println(format("{0} : elpase[{1}], latency[{2, number}ns], TPS[{3, number,#.##}]",
                name, stopwatch, latency, tps));
    }
}
