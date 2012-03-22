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

package com.github.zhongl.ephemeron;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FlowController {

    private final Semaphore semaphore;
    private final long timeout;
    private final TimeUnit unit;

    public FlowController(int throughout, long timeout, TimeUnit unit) {
        this.semaphore = new Semaphore(throughout, true);
        this.timeout = timeout;
        this.unit = unit;
    }

    public boolean tryAcquire() {
        try {
            return semaphore.tryAcquire(timeout, unit);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    public void release() {
        semaphore.release();
    }

    public int throughout(int delta) {
        if (delta > 0) semaphore.release(delta);
        if (delta < 0) semaphore.acquireUninterruptibly(-delta);
        return semaphore.availablePermits();
    }
}
