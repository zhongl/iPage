package com.github.zhongl.ex.actor;

import java.util.HashMap;
import java.util.Map;

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
}

