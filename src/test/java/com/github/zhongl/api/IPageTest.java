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

package com.github.zhongl.api;

import com.github.zhongl.util.CallbackFuture;
import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.FutureCallbacks;
import com.github.zhongl.util.Md5;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Test;
import org.softee.management.helper.ObjectNameBuilder;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageTest extends FileTestContext {

    private IPage<String, String> iPage;

    @Test
    public void flushByElapseMillis() throws Exception {
        dir = testDir("flushByElapseMillis");
        iPage = stringIPage(dir, 10, 1000, 2L);

        String key = "key";
        String value = "value";

        CallbackFuture<Void> future = new CallbackFuture<Void>();
        iPage.add(key, value, future);
        future.get();

        assertThat(iPage.get(key), is(value));
    }

    @Test
    public void flushByCount() throws Exception {
        dir = testDir("flushByCount");
        iPage = stringIPage(dir, 10, 1, Long.MAX_VALUE);

        String key = "key";

        CallbackFuture<Void> future = new CallbackFuture<Void>();
        iPage.remove(key, future);
        future.get();

        assertThat(iPage.get(key), is(nullValue()));
    }

    @Test
    public void flushByFlowControl() throws Exception {
        dir = testDir("flushByFlowControl");
        iPage = stringIPage(dir, 1, Integer.MAX_VALUE, Long.MAX_VALUE);

        String key = "key";
        String value = "value";

        CallbackFuture<Void> future = new CallbackFuture<Void>();
        iPage.add(key, value, future);
        iPage.add("trigger", "flow control", FutureCallbacks.<Void>ignore());
        future.get();

        assertThat(iPage.get(key), is(value));
    }

    @Test
    public void mbeanRegistration() throws Exception {
        dir = testDir("mbeanRegistration");
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        iPage = stringIPage(dir, 1, 1, Long.MAX_VALUE);

        String domain = "com.github.zhongl.ipage";
        String name = iPage.toString();
        ObjectName ephemerons = new ObjectNameBuilder(domain).withType("Ephemerons")
                                                             .withName(name)
                                                             .build();
        ObjectName storage = new ObjectNameBuilder(domain).withType("Storage")
                                                          .withName(name)
                                                          .build();
        ObjectName defragPolicy = new ObjectNameBuilder(domain).withType("DefragPolicy")
                                                               .withName(name)
                                                               .build();

        server.getMBeanInfo(ephemerons);
        server.getMBeanInfo(defragPolicy);

        iPage.stop();

        try {
            server.getMBeanInfo(ephemerons);
            fail("MBean should be unregistered.");
        } catch (InstanceNotFoundException e) { }

        try {
            server.getMBeanInfo(storage);
            fail("MBean should be unregistered.");
        } catch (InstanceNotFoundException e) { }

        try {
            server.getMBeanInfo(defragPolicy);
            fail("MBean should be unregistered.");
        } catch (InstanceNotFoundException e) { }

    }

    @Test
    public void removeOnAdding() throws Exception {
        dir = testDir("removeOnAdding");

        iPage = stringIPage(dir, 10000, 1000, 100L);

        final BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

        final int times = 10000;

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < times; i++) {
                    String key = i + "";
                    iPage.add(key, key, FutureCallbacks.<Void>ignore());
                    try {
                        queue.put(key);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        int count = 0;
        final CountDownLatch latch = new CountDownLatch(times);
        while (count++ < times) {
            String key = queue.take();
            iPage.remove(key, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
            });
        }

        while (!latch.await(3L, TimeUnit.SECONDS)) {
            System.out.println("rest = " + latch.getCount());
        }

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        String domain = "com.github.zhongl.ipage";
        String name = iPage.toString();
        ObjectName ephemerons = new ObjectNameBuilder(domain).withType("Ephemerons")
                                                             .withName(name)
                                                             .build();
        ObjectName defragPolicy = new ObjectNameBuilder(domain).withType("DefragPolicy")
                                                               .withName(name)
                                                               .build();


        while ((Integer) server.getAttribute(ephemerons, "size") != 0) {
            Thread.sleep(500L);
        }

        assertThat((Integer) server.getAttribute(defragPolicy, "lastAliveSize"), is(0));

    }

    @Test
    public void issue52() throws Exception {
        // Fixed #52 : Iterated value incorrect

        dir = testDir("issue52");

        iPage = stringIPage(dir, 11, 1000, 100L);

        final int times = 20;

        final CountDownLatch latch = new CountDownLatch(times);

        for (int i = 0; i < times; i++) {
            String key = i + "";
            iPage.add(key, key, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void result) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    t.printStackTrace();
                    latch.countDown();
                }
            });
        }

        latch.await();

        Iterator<String> iterator = iPage.iterator();
        for (int i = 0; i < times; i++) {
            assertThat(iterator.next(), is(i + ""));
        }

    }

    @Override
    @After
    public void tearDown() throws Exception {
        iPage.stop();
        super.tearDown();
    }

    private IPage<String, String> stringIPage(File dir, int throughout, int flushCount, long flushMillis) throws Exception {
        IPage<String, String> stringIPage = new IPage<String, String>(dir, new StringCodec(), throughout, flushMillis, flushCount) {

            @Override
            protected Md5Key transform(String key) {
                return new Md5Key(Md5.md5(key.getBytes()));
            }
        };
        stringIPage.start();
        return stringIPage;
    }

}
