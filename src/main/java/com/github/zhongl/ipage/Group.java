package com.github.zhongl.ipage;

import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.LinkedList;
import java.util.Queue;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@NotThreadSafe
public abstract class Group {

    public static final Group NULL = new Group() {
        @Override
        public void commit() {}

        @Override
        public void rollback(Exception e) { }

        @Override
        public <T> FutureCallback<T> decorate(FutureCallback<T> callback) {
            return callback;
        }

        @Override
        public void register(FutureCallback<?> callback) { }
    };

    public abstract void commit();

    public static Group newInstance() {
        return new DefaultGroup();
    }

    public abstract void rollback(Exception e);

    /**
     * Decorate {@link com.google.common.util.concurrent.FutureCallback} to a new one, which can be grouped for
     * committing or rollbacking.
     *
     * @param callback {@link com.google.common.util.concurrent.FutureCallback}
     * @param <T>
     *
     * @return groupable {@link com.google.common.util.concurrent.FutureCallback}
     */
    public abstract <T> FutureCallback<T> decorate(FutureCallback<T> callback);

    public abstract void register(FutureCallback<?> callback);

    private static class GroupableFutureCallback<T> implements FutureCallback<T> {

        private final FutureCallback<T> delegate;
        private T result;

        private GroupableFutureCallback(FutureCallback<T> delegate) {this.delegate = delegate;}

        @Override
        public void onSuccess(T result) {
            this.result = result;
        }

        @Override
        public void onFailure(Throwable t) {
            delegate.onFailure(t); // trigger failure immediately if it is write exception
        }

        void commit() {
            delegate.onSuccess(result);
        }

        void rollback(Exception e) {
            delegate.onFailure(e); // trigger rollback failure
        }
    }

    private static class DefaultGroup extends Group {

        private final Queue<GroupableFutureCallback<?>> callbacks = new LinkedList<GroupableFutureCallback<?>>();

        @Override
        public void commit() {
            while (true) {
                GroupableFutureCallback<?> callback = callbacks.poll();
                if (callback == null) break;
                callback.commit();
            }
        }


        @Override
        public void rollback(Exception e) {
            while (true) {
                GroupableFutureCallback<?> callback = callbacks.poll();
                if (callback == null) break;
                callback.rollback(e);
            }
        }

        @Override
        public <T> FutureCallback<T> decorate(FutureCallback<T> callback) {
            return new GroupableFutureCallback<T>(callback);
        }

        @Override
        public void register(FutureCallback<?> callback) {
            if (callback instanceof GroupableFutureCallback) {
                callbacks.add((GroupableFutureCallback<?>) callback);
            }
        }
    }

}
