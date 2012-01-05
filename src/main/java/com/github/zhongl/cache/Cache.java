/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.cache;

import com.github.zhongl.journal.Event;
import com.github.zhongl.journal.Events;
import com.github.zhongl.page.Page;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class Cache<K, V> {
    private final Events<K, V> events;
    private final com.google.common.cache.Cache<K, V> gCache;
    private final Map<K, V> fresh;

    public Cache(
            Events<K, V> events,
            final Durable<K, V> durable,
            int capacity,
            long durationMilliseconds
    ) {
        this.events = events;
        int concurrentLevel = Runtime.getRuntime().availableProcessors() * 2;
        fresh = new ConcurrentHashMap<K, V>(16, 0.75f, concurrentLevel);
        gCache = CacheBuilder.newBuilder()
                             .maximumSize(capacity)
                             .concurrencyLevel(concurrentLevel)
                             .expireAfterWrite(durationMilliseconds, TimeUnit.MILLISECONDS)
                             .build(new CacheLoader<K, V>() {
                                 @Override
                                 public V load(K key) throws Exception {
                                     V value = fresh.get(key);
                                     if (value != null) return value;
                                     return durable.load(key);
                                 }
                             });
    }

    public void apply(Page<Event> page) {
        for (Event event : page) apply(event);
    }

    public void apply(Event event) {
        if (events.isAdd(event)) {
            fresh.put(events.getKey(event), events.getValue(event));
        } else {
            K key = events.getKey(event);
            fresh.remove(key);
            gCache.invalidate(key);
        }
    }

    public V get(K key) {
        try {
            return gCache.get(key);
        } catch (Exception e) {
            return null;
        }
    }

    public void weak(Event event) {
        fresh.remove(events.getKey(event));
    }

    public long size() {return gCache.size();}

    public void cleanUp() {
        fresh.clear();
        gCache.invalidateAll();
    }
}
