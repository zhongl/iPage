package com.github.zhongl.ex.util;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FutureCallbacks {
    private FutureCallbacks() {}

    public static <T> FutureCallback<T> ignore() {
        return new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) { }

            @Override
            public void onFailure(Throwable t) { }
        };
    }

    public static <T> T call(Function<FutureCallback<T>, Void> function) {
        CallbackFuture<T> callback = new CallbackFuture<T>();
        function.apply(callback);
        return getUnchecked(callback);
    }

    public static <T> T getUnchecked(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }
}
