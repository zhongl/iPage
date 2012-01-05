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

package com.github.zhongl.journal;

import com.github.zhongl.cache.Cache;
import com.github.zhongl.durable.DurableEngine;
import com.github.zhongl.engine.CallByCountOrElapse;
import com.github.zhongl.engine.Engine;
import com.github.zhongl.engine.Group;
import com.github.zhongl.engine.Task;
import com.github.zhongl.page.Accessor;
import com.github.zhongl.util.FilesLoader;
import com.github.zhongl.util.NumberNamedFilterAndComparator;
import com.github.zhongl.util.Transformer;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Journal {

    private static final int BACKLOG = Integer.getInteger("ipage.jounal.engine.backlog", 256);
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private final File dir;
    private final Accessor<Event> accessor;
    private final InnerEngine engine;
    private final Group group;
    private final DurableEngine durableEngine;
    private final Cache cache;
    private final CallByCountOrElapse callByCountOrElapse;

    private volatile EventPage currentEventPage;

    public Journal(
            File dir,
            Accessor<Event> accessor,
            DurableEngine durableEngine,
            Cache cache,
            int flushCount,
            long flushElapseMilliseconds,
            boolean groupCommit
    ) {
        this.dir = dir;
        this.accessor = accessor;
        this.durableEngine = durableEngine;
        this.cache = cache;
        this.callByCountOrElapse = new CallByCountOrElapse(flushCount, flushElapseMilliseconds, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                currentEventPage.fix();
                if (isGroupCommit()) Journal.this.cache.apply(currentEventPage);
                Journal.this.durableEngine.apply(currentEventPage);
                currentEventPage = createNewEventPage();
                return null;
            }
        });
        this.group = groupCommit ? Group.newInstance() : Group.NULL;
        this.engine = new InnerEngine(flushElapseMilliseconds / 2, TIME_UNIT, BACKLOG);
    }

    public void open() throws IOException {
        for (EventPage eventPage : loadUnappliedPages()) {
            cache.apply(eventPage);
            durableEngine.apply(eventPage);
        }
        this.currentEventPage = createNewEventPage();
        engine.startup();
    }

    public void close() throws IOException {
        currentEventPage.fix();
        engine.shutdown();
    }

    public boolean append(Event event) {
        return engine.append(event);
    }

    private LinkedList<EventPage> loadUnappliedPages() throws IOException {
        if (!dir.exists()) dir.mkdirs();
        return new FilesLoader<EventPage>(
                dir,
                new NumberNamedFilterAndComparator(),
                new Transformer<EventPage>() {
                    @Override
                    public EventPage transform(File file, boolean last) throws IOException {
                        try {
                            currentEventPage = new EventPage(file, accessor, cache);
                            return currentEventPage;
                        } catch (IllegalStateException e) {
                            file.delete();
                            return null;
                        }
                    }
                }
        ).loadTo(new LinkedList<EventPage>());
    }

    private EventPage createNewEventPage() throws IOException {
        String child = currentEventPage == null ? "0" : Long.toString(currentEventPage.number() + 1);
        return new EventPage(new File(dir, child), accessor, cache);
    }

    private Task<Void> task(final Event event) {
        return new Task<Void>(group.decorate(event)) {
            @Override
            protected Void execute() throws Throwable {
                currentEventPage.add(event);
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

    private void tryGroupCommitByElapse() {
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

        public boolean append(final Event event) {
            return submit(task(event));
        }
    }
}
