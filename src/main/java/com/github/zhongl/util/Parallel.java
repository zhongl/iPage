package com.github.zhongl.util;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;
import org.softee.management.annotation.ManagedOperation;
import org.softee.management.helper.MBeanRegistration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Parallel {
    private static final int CORE_SIZE = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_MAXIMUM_POOL_SIZE = 100;
    private static final int DEFAULT_KEEP_ALIVE_TIME = 60;

    private static final JmxBean JMX_BEAN;
    private static final Semaphore SEMAPHORE;
    private static final ThreadPoolExecutor IO_BOUND_EXECUTOR;
    private static final ThreadPoolExecutor CPU_BOUND_EXECUTOR;


    static {
        JMX_BEAN = new JmxBean();
        SEMAPHORE = new Semaphore(10 * CORE_SIZE);

        CPU_BOUND_EXECUTOR = new ThreadPoolExecutor(
                CORE_SIZE,
                CORE_SIZE,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new NamedDaemonThreadFactory("Parallel-CPU"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        ) {
            @Override
            protected void beforeExecute(Thread t, Runnable r) {
                if (!SEMAPHORE.tryAcquire()) getRejectedExecutionHandler().rejectedExecution(t, this);
            }

            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                SEMAPHORE.release();
            }
        };

        IO_BOUND_EXECUTOR = new ThreadPoolExecutor(
                CORE_SIZE,
                DEFAULT_MAXIMUM_POOL_SIZE,
                DEFAULT_KEEP_ALIVE_TIME,
                SECONDS,
                new SynchronousQueue<Runnable>(),
                new NamedDaemonThreadFactory("Parallel-IO"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                IO_BOUND_EXECUTOR.shutdownNow();
                CPU_BOUND_EXECUTOR.shutdownNow();
                try { new MBeanRegistration(JMX_BEAN).unregister(); } catch (Exception ignored) { }
            }
        });

        try {
            new MBeanRegistration(JMX_BEAN).register();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static <I, O> Collection<O> map(Collection<I> collection, final Function<I, O> function, ExecutorService service)
            throws InterruptedException {

        return Collections2.transform(
                service.invokeAll(
                        Collections2.transform(
                                collection,
                                new Function<I, Callable<O>>() {
                                    @Override
                                    public Callable<O> apply(@Nullable final I input) {
                                        return new Callable<O>() {
                                            @Override
                                            public O call() throws Exception {
                                                return function.apply(input);
                                            }
                                        };
                                    }
                                }
                        )
                ),
                new Function<Future<O>, O>() {
                    @Override
                    public O apply(@Nullable Future<O> future) {
                        try {
                            return future.get();
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                }
        );
    }

    @MBean(objectName = "com.github.zhongl.ipage:type=Parallel")
    private static class JmxBean {
        @ManagedAttribute
        public void setIOBoundExecutorMaxPoolSize(int value) {
            IO_BOUND_EXECUTOR.setMaximumPoolSize(value);
        }

        @ManagedAttribute
        public int getIOBoundExecutorMaxPoolSize() {
            return IO_BOUND_EXECUTOR.getMaximumPoolSize();
        }

        @ManagedAttribute
        public void setIOBoundExecutorKeepAliveSeconds(int value) {
            IO_BOUND_EXECUTOR.setKeepAliveTime(value, SECONDS);
        }

        @ManagedAttribute
        public int getIOBoundExecutorKeepAliveSeconds() {
            return (int) IO_BOUND_EXECUTOR.getKeepAliveTime(SECONDS);
        }

        @ManagedOperation
        public int adjustCPUBoundBacklog(int delta) {
            if (delta > 0) SEMAPHORE.release(delta);
            if (delta < 0) SEMAPHORE.acquireUninterruptibly(-delta);
            return SEMAPHORE.availablePermits();
        }
    }

    public static ExecutorService ioBoundExecutor() {
        return IO_BOUND_EXECUTOR;
    }

    public static ExecutorService cpuBoundExecutor() {
        return CPU_BOUND_EXECUTOR;
    }


    private Parallel() {}

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

}
