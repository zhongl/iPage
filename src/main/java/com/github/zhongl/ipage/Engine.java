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

    public static final Shutdown SHUTDOWN = new Shutdown();

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
            tasks.put(SHUTDOWN);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        }
    }

    public void awaitForShutdown(long timeout) throws InterruptedException {
        core.join(timeout);
    }

    public boolean isRunning() {
        return core.isAlive();
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
                    if (task instanceof Shutdown) break;
                    task.run();
                } catch (InterruptedException e) {
                    interrupted = true;
                    continue;
                }
            }
            if (interrupted) Thread.currentThread().interrupted();
        }

    }

    /** Overwrite this method for some time-sensitive stuff. */
    protected void hearbeat() {}

    private static class Shutdown implements Runnable {
        @Override
        public void run() { }
    }

    /** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
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
