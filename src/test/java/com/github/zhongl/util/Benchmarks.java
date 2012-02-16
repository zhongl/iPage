/*
 * Copyright 2012 zhongl
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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
