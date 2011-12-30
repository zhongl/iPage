/*
 * Copyright 2011 zhongl
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
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CacheTest {
    @Test
    public void main() throws Exception {
        Event event = mock(Event.class);
        EventToKeyValue<String, String> eventToKeyValue = mock(EventToKeyValue.class);

        String key = "key";
        String value = "value";
        doReturn(key).when(eventToKeyValue).getKey(event);
        doReturn(value).when(eventToKeyValue).getValue(event);

        Durable<String, String> durable = mock(Durable.class);
        long durationMilliseconds = 10L;

        Cache<String, String> cache = new Cache<String, String>(eventToKeyValue, durable, 16, durationMilliseconds);
        cache.apply(event);
        assertThat(cache.get(key), is(value));

        Thread.sleep(durationMilliseconds);
        cache.weak(key);
        assertThat(cache.get(key), is(nullValue()));

        doReturn(value).when(durable).load(key);
        assertThat(cache.get(key), is(value));

        cache.cleanUp();
        doReturn(null).when(durable).load(key);
        assertThat(cache.get(key), is(nullValue()));
    }
}
