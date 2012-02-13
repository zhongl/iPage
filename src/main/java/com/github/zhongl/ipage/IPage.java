package com.github.zhongl.ipage;

import com.github.zhongl.util.CallByCountOrElapse;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import org.softee.management.helper.MBeanRegistration;

import javax.annotation.Nullable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class IPage<K, V> extends Actor implements Iterable<V> {

    private static final String EPHEMERONS = "Ephemerons";
    private static final String STORAGE = "Storage";

    private final Storage<V> storage;
    private final Ephemerons<V> ephemerons;
    private final QuanlityOfService quanlityOfService;
    private final CallByCountOrElapse callByCountOrElapse;

    public IPage(File dir,
                 QuanlityOfService quanlityOfService,
                 Codec<V> codec,
                 int ephemeronThroughout,
                 long flushMillis,
                 int flushCount) throws Exception {

        super((flushMillis / 2));
        this.quanlityOfService = quanlityOfService;
        this.storage = new Storage<V>(dir, codec);
        this.ephemerons = new Ephemerons<V>(new ConcurrentHashMap<Key, Ephemerons<V>.Record>()) {
            @Override
            protected void requestFlush(
                    final Collection<Entry<Key, V>> appendings,
                    final Collection<Key> removings,
                    final FutureCallback<Void> flushedCallback
            ) {
                submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        storage.merge(appendings, removings, flushedCallback);
                        return Nils.VOID;
                    }
                });
            }

            @Override
            protected V getMiss(Key key) {
                return storage.get(key);
            }

        };

        ephemerons.throughout(ephemeronThroughout);

        this.callByCountOrElapse = new CallByCountOrElapse(flushCount, flushMillis, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ephemerons.flush();
                return Nils.VOID;
            }
        });

        new MBeanRegistration(ephemerons, objectName(EPHEMERONS)).register();
        new MBeanRegistration(storage, objectName(STORAGE)).register();
    }

    /**
     * @param key
     * @param value
     *
     * @throws IllegalStateException cause by {@link QuanlityOfService#RELIABLE}
     */
    public void add(final K key, final V value) {
        quanlityOfService.call(new Function<FutureCallback<Void>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<Void> removedOrDurableCallback) {
                ephemerons.add(transform(key), value, removedOrDurableCallback);
                return null;
            }
        });
        tryCallByCount();
    }

    public void remove(K key) {
        ephemerons.remove(transform(key));
        tryCallByCount();
    }

    public V get(K key) {
        return ephemerons.get(transform(key));
    }

    @Override
    public Iterator<V> iterator() {
        return storage.iterator();
    }

    @Override
    public void stop() {
        super.stop();
        try {
            new MBeanRegistration(ephemerons, objectName(EPHEMERONS)).unregister();
            new MBeanRegistration(storage, objectName(STORAGE)).unregister();
        } catch (Exception ignored) { }
    }

    @Override
    protected void heartbeat() throws Throwable {
        callByCountOrElapse.tryCallByElapse();
    }

    @Override
    protected boolean onInterruptedBy(Throwable t) {
        return super.onInterruptedBy(t);    // TODO log error
    }

    protected abstract Key transform(K key);

    private void tryCallByCount() {
        submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                callByCountOrElapse.tryCallByCount();
                return Nils.VOID;
            }
        });
    }

    private ObjectName objectName(String ephemerons1) throws MalformedObjectNameException {
        return new ObjectName(MessageFormat.format("com.github.zhongl.ipage:type={0},belongs={1}", ephemerons1, this));
    }
}
