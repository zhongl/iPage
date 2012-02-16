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

package com.github.zhongl.ipage;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * {@link Actor} is thread-bound {@link Runnable} executor.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public abstract class Actor {

    public static final long TIMEOUT = Long.getLong("ipage.actor.poll.timeout", 500L);

    private final Core core;
    private final BlockingQueue<Runnable> tasks; // TODO monitor

    private final Runnable SHUTDOWN = new Runnable() {
        public void run() { core.running = false; }
    };
    private final long timeout;

    protected Actor(String name) {
        this(name, TIMEOUT);
    }

    public Actor(String name, long timeout) {
        this.timeout = timeout;
        this.tasks = new LinkedBlockingQueue<Runnable>();
        core = new Core(name);
        core.start();
    }

    public synchronized void stop() {
        if (!core.isAlive()) return;
        try {
            tasks.put(SHUTDOWN);
            core.join();
        } catch (InterruptedException ignored) { }
    }

    protected final <T> Future<T> submit(Callable<T> task) {
        final FutureTask futureTask = new FutureTask(task);
        tasks.offer(futureTask);
        return futureTask;
    }

    /** Overwrite this method for some time-sensitive stuff. */
    protected void heartbeat() throws Throwable {}

    protected boolean onInterruptedBy(Throwable t) { return true; }

    private class Core extends Thread {

        @GuardedBy("this")
        private Boolean running = true;

        public Core(String name) {
            super(name);
        }

        @Override
        public void run() {
            while (running) {
                try {
                    Runnable task = tasks.poll(timeout, MILLISECONDS);
                    heartbeat();
                    if (task == null) continue;
                    task.run();
                } catch (Throwable t) {
                    running = onInterruptedBy(t);

                }
            }
        }

    }

}
