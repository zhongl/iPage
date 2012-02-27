package com.github.zhongl.util;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ParallelExecutor {
    public static final int CORE_SIZE = Runtime.getRuntime().availableProcessors();
    private final ExecutorService service;

    public static ParallelExecutor executorForIO(String name, int maxThreadSize, long keepAliveSeconds) {
        return new ParallelExecutor(
                new ThreadPoolExecutor(
                        CORE_SIZE,
                        maxThreadSize,
                        keepAliveSeconds,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<Runnable>(),
                        new NamedDaemonThreadFactory(name),
                        new ThreadPoolExecutor.CallerRunsPolicy()
                ));
    }

    public static ParallelExecutor executorForCompute(String name, int maxLoad) {
        return new ParallelExecutor(
                new ThreadPoolExecutor(
                        CORE_SIZE,
                        CORE_SIZE,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<Runnable>(maxLoad * CORE_SIZE),
                        new NamedDaemonThreadFactory(name),
                        new ThreadPoolExecutor.CallerRunsPolicy()
                ));
    }

    private ParallelExecutor(ExecutorService service) {this.service = service;}

    public <I, O> Collection<O> map(Collection<I> list, Function<I, Callable<O>> function) throws InterruptedException {
        return Collections2.transform(service.invokeAll(Collections2.transform(list, function)), new GetFuture<O>());
    }

    private static class NamedDaemonThreadFactory implements ThreadFactory {
        private final String name;

        public NamedDaemonThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(name + "-" + t.getId());
            return t;
        }
    }

    private class GetFuture<O> implements Function<Future<O>, O> {
        @Override
        public O apply(@Nullable Future<O> future) {
            try {
                return future.get();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
