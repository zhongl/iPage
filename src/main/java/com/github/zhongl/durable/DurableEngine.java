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

package com.github.zhongl.durable;

import com.github.zhongl.engine.Engine;
import com.github.zhongl.journal.Event;
import com.github.zhongl.page.Page;

import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DurableEngine extends Engine {
    public DurableEngine(long flushElapse, TimeUnit unit, int backlog) {
        super(flushElapse / 2, unit, backlog);
    }

    public void apply(Page<Event> page) {
        for (Event event : page) apply(event);
        page.clear();
    }

    private void apply(Event event) {
        // TODO apply
    }
}
