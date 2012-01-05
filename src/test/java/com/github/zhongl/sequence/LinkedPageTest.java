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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessor;
import com.github.zhongl.page.Accessors;
import com.github.zhongl.page.ReadOnlyChannels;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class LinkedPageTest extends FileBase {

    private LinkedPage<String> linkedPage;

    @Override
    @After
    public void tearDown() throws Exception {
        if (linkedPage != null) linkedPage.close();
        super.tearDown();
    }

    @Test
    public void main() throws Exception {
        dir = testDir("main");
        Accessor<String> accessor = Accessors.STRING;
        linkedPage = new LinkedPage<String>(new File(dir, "0"), accessor, new ReadOnlyChannels());

        String record = "record";
        Cursor cursor = linkedPage.append(record);
        assertThat(linkedPage.get(cursor), is(record));
    }

    @Test
    public void splitCase1() throws Exception {
        dir = testDir("splitCase1");
        newLinkedPage();

        List<LinkedPage<String>> linkedPages = linkedPage.split(new Cursor(32L), new Cursor(64L));
        assertThat(linkedPages.size(), is(2));
        assertThat(new File(dir, "0").length(), is(32L));
        assertThat(new File(dir, "64").length(), is(4032L));

        close(linkedPages);
    }

    @Test
    public void splitCase2() throws Exception {
        dir = testDir("splitCase2");
        newLinkedPage();

        List<LinkedPage<String>> linkedPages = linkedPage.split(new Cursor(0L), new Cursor(64L));
        assertThat(linkedPages.size(), is(1));
        assertNotExistFile("0");
        assertThat(new File(dir, "64").length(), is(4032L));

        close(linkedPages);
    }

    @Test
    public void splitCase3() throws Exception {
        dir = testDir("splitCase3");
        newLinkedPage();

        List<LinkedPage<String>> linkedPages = linkedPage.split(new Cursor(16L), new Cursor(30L));
        assertThat(linkedPages.get(0), is(linkedPage));
    }

    @Test
    public void leftCase1() throws Exception {
        dir = testDir("leftCase1");
        newLinkedPage();

        LinkedPage<String> left = linkedPage.left(new Cursor(16L));
        assertThat(left, is(not(linkedPage)));
        linkedPage = left; // let linkedPage be close when tear down
        assertThat(new File(dir, "0").length(), is(16L));
    }

    @Test
    public void leftCase2() throws Exception {
        dir = testDir("leftCase2");
        newLinkedPage();

        linkedPage = linkedPage.left(new Cursor(0L));
        assertThat(linkedPage, is(nullValue()));
        assertNotExistFile("0");
    }

    @Test
    public void leftCase2WithOffsetLessThanLinkedPageBeginPosition() throws Exception {
        dir = testDir("leftCase2");
        newLinkedPage(4096);

        linkedPage = linkedPage.left(new Cursor(4095L));
        assertThat(linkedPage, is(nullValue()));
        assertNotExistFile("0");
    }

    @Test
    public void rightCase1() throws Exception {
        dir = testDir("rightCase1");
        newLinkedPage();

        linkedPage = linkedPage.right(new Cursor(64L));
        assertThat(linkedPage, is(notNullValue()));
        assertNotExistFile("0");
        assertThat(new File(dir, "64").length(), is(4032L));
    }

    @Test
    public void rightCase1WithNonZeroBeginPosition() throws Exception {
        dir = testDir("rightCase1WithNonZeroBeginPosition");
        newLinkedPage(4096);

        linkedPage = linkedPage.right(new Cursor(4128L));
        assertThat(linkedPage, is(notNullValue()));
        assertNotExistFile("4096");
        assertThat(new File(dir, "4128").length(), is(4064L));
    }

    @Test
    public void rightCase2() throws Exception {
        dir = testDir("rightCase2");
        newLinkedPage();

        LinkedPage<String> right = linkedPage.right(new Cursor(0L));
        assertThat(right, is(linkedPage));
        assertThat(new File(dir, "0").length(), is(4096L));
    }

    private void close(List<LinkedPage<String>> linkedPages) throws IOException {
        for (LinkedPage<String> c : linkedPages) c.close();
    }

    protected void newLinkedPage() throws IOException {
        newLinkedPage(0);
    }

    private void newLinkedPage(int beginPosition) throws IOException {
        dir.mkdirs();
        linkedPage = new LinkedPage<String>(new File(dir, beginPosition + ""), Accessors.STRING, new ReadOnlyChannels());
        for (int i = 0; i < 256; i++) {
            linkedPage.append("0123456789ab");
        }
        linkedPage.fix();
    }

}
