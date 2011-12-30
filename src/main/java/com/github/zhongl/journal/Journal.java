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

package com.github.zhongl.journal;

import com.github.zhongl.cache.Cache;
import com.github.zhongl.durable.DurableEngine;
import com.github.zhongl.engine.Engine;
import com.github.zhongl.engine.Task;
import com.google.common.util.concurrent.FutureCallback;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Journal {

    private static final int BACKLOG = Integer.getInteger("jounal.backlog", 256);
    private final static TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final PageRepository pageRepository;
    private final InnerEngine engine;
    private final Group group;
    private final DurableEngine durableEngine;
    private final Cache cache;
    private final CallByCountOrElapse callByCountOrElapse;

    private final Callable<?> flusher = new Callable<Object>() {
        @Override
        public Object call() throws Exception {
            currentPage.fix();
            if (isGroupCommit()) cache.apply(currentPage);
            durableEngine.apply(currentPage);
            currentPage = pageRepository.create();
            return null;
        }
    };

    private volatile Page currentPage;

    public Journal(
            PageRepository pageRepository,
            DurableEngine durableEngine,
            Cache cache,
            int flushCount,
            long flushElapseMilliseconds,
            boolean groupCommit
    ) {
        this.pageRepository = pageRepository;
        this.durableEngine = durableEngine;
        this.cache = cache;
        this.callByCountOrElapse = new CallByCountOrElapse(flushCount, flushElapseMilliseconds, flusher);
        this.group = groupCommit ? Group.newInstance() : Group.NULL;
        this.engine = new InnerEngine(flushElapseMilliseconds / 2, TIME_UNIT, BACKLOG);
    }

    public void open() {
        this.currentPage = pageRepository.create();
        for (Page page : pageRepository.unappliedPages()) {
            cache.apply(page);
            durableEngine.apply(page);
        }
        engine.startup();
    }

    public void close() throws IOException {
        currentPage.fix();
        engine.shutdown();
    }

    public void append(Event event) {
        engine.append(event);
    }

    private Task<Void> task(final Event event) {
        FutureCallback<Void> callback = new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) { event.onCommit(); }

            @Override
            public void onFailure(Throwable t) { event.onError(t); }
        };

        return new Task<Void>(group.decorate(callback)) {
            @Override
            protected Void execute() throws Throwable {
                currentPage.add(event);
                if (!isGroupCommit()) cache.apply(event);
                tryGroupCommitByCount();
                return null;
            }
        };
    }

    private boolean isGroupCommit() {return Group.NULL != group;}

    private void tryGroupCommitByCount() {
        try {
            if (callByCountOrElapse.tryCallByCount()) group.commit();
        } catch (Throwable t) {
            group.rollback(t);
        }
    }

    public void tryGroupCommitByElapse() {
        try {
            if (callByCountOrElapse.tryCallByElapse()) group.commit();
        } catch (Throwable t) {
            group.rollback(t);
        }
    }

    private class InnerEngine extends Engine {

        public InnerEngine(long timeout, TimeUnit unit, int backlog) {
            super(timeout, unit, backlog);
        }

        @Override
        protected void hearbeat() {
            tryGroupCommitByElapse();
        }

        public void append(final Event event) {
            submit(task(event));
        }
    }
}
