/*
 * Copyright 2011 zhongl
 *
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

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * {@link Engine} is thread-bound {@link Runnable} executor.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public abstract class Engine {

    public static final Runnable SHUTDOWN = new Runnable() {
        public void run() {}
    };

    private final BlockingQueue<Runnable> tasks; // TODO monitor
    private final long timeout;
    private final TimeUnit timeUnit;
    private final Core core;

    public Engine(long timeout, TimeUnit unit, int backlog) {
        this.timeout = timeout;
        this.timeUnit = unit;
        this.tasks = new LinkedBlockingQueue<Runnable>(backlog);
        core = new Core(getClass().getSimpleName());
    }

    public void startup() {
        core.start();
    }

    public void shutdown() {
        try {
            if (core.isAlive())
                tasks.put(SHUTDOWN);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    public void awaitForShutdown(long timeout) throws InterruptedException {
        core.join(timeout);
    }

    protected final boolean submit(Task<?> task) {
        return tasks.offer(task);
    }

    private class Core extends Thread {

        public Core(String name) {
            super(name);
        }

        @Override
        public void run() {
            boolean interrupted = false;
            while (true) {
                try {
                    Runnable task = tasks.poll(timeout, timeUnit);
                    hearbeat();
                    if (task == null) continue;
                    if (task == SHUTDOWN) break;
                    task.run();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }

    }

    /** Overwrite this method for some time-sensitive stuff. */
    protected void hearbeat() {}

    public abstract static class Task<T> implements Runnable {
        protected final FutureCallback<T> callback;

        public Task(FutureCallback<T> callback) {this.callback = callback;}

        @Override
        public final void run() {
            try {
                T result = execute();
                callback.onSuccess(result);
            } catch (Throwable t) {
                callback.onFailure(t);
            }
        }

        protected abstract T execute() throws Throwable;
    }
}
