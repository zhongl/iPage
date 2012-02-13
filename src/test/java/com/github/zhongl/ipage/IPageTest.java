/*
 * Copyright 2012 zhongl
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

package com.github.zhongl.ipage;

import com.github.zhongl.util.FileTestContext;
import com.github.zhongl.util.Md5;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class IPageTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");

        long flushMillis = 100L;
        int flushCount = 1000;
        IPage<String, String> iPage;
        int ephemeronThroughout = 1;
        iPage = new IPage<String, String>(dir, new StringCodec(), ephemeronThroughout, flushMillis, flushCount) {

            @Override
            protected Key transform(String key) {
                return new Key(Md5.md5(key.getBytes()));
            }
        };

        String key = "key";
        String value = "value";

        QuanlityOfService<String, String> service = new QuanlityOfService<String, String>(iPage);

        service.sendAdd(key, value);
        assertThat(iPage.get(key), is(value));
        service.sendRemove(key);
        assertThat(iPage.get(key), is(nullValue()));

        service.callAdd(key, value);
        assertThat(iPage.get(key), is(value));
        service.callRemove(key);
        assertThat(iPage.get(key), is(nullValue()));

        service.futureAdd(key, value).get();
        assertThat(iPage.get(key), is(value));
        service.futureRemove(key).get();
        assertThat(iPage.get(key), is(nullValue()));

        iPage.stop();
    }

}
