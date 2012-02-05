package com.github.zhongl.ex.actor;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.synchronizedMap;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Actors {
    private static final Map<Class<?>, Actor> flyweight = synchronizedMap(new HashMap<Class<?>, Actor>());

    public static <T> T actor(Class<T> type) {
        return (T) flyweight.get(type);
    }

    public static void register(Actor actor) {
        for (Class<?> anInterface : actor.getClass().getInterfaces()) {
            if (anInterface.getAnnotation(Asynchronize.class) != null)
                flyweight.put(anInterface, actor);
        }
    }

    public static <T> T sync(Function<FutureCallback<T>, Void> function) {
        CallbackFuture<T> callback = new CallbackFuture<T>();
        function.apply(callback);
        try {
            return callback.get();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }

    }
}

