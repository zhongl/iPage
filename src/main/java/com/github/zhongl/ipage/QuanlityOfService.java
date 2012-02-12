package com.github.zhongl.ipage;

import com.github.zhongl.util.CallbackFuture;
import com.github.zhongl.util.FutureCallbacks;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
enum QuanlityOfService {
    RELIABLE {
        @Override
        public void call(Function<FutureCallback<Void>, Void> function) {
            CallbackFuture<Void> future = new CallbackFuture<Void>();
            function.apply(future);
            FutureCallbacks.getUnchecked(future);
        }
    }, LATENCY {
        @Override
        public void call(Function<FutureCallback<Void>, Void> function) {
            function.apply(FutureCallbacks.<Void>ignore());
        }
    };

    public abstract void call(Function<FutureCallback<Void>, Void> function);
}
