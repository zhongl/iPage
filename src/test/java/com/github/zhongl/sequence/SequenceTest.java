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

package com.github.zhongl.sequence;

import com.github.zhongl.page.Accessors;
import com.github.zhongl.page.ReadOnlyChannels;
import com.github.zhongl.util.FileBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class SequenceTest extends FileBase {

    Sequence<String> sequence;
    private LinkedList<LinkedPage<String>> linkedPages;

    @Before
    public void setUp() throws Exception {
        linkedPages = new LinkedList<LinkedPage<String>>();
        sequence = new Sequence<String>(linkedPages, 16L);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        sequence.close();
        super.tearDown();
    }

    @Test
    public void main() throws Exception {
        dir = testDir("main");

        linkedPages.add(new LinkedPage<String>(new File(dir, "0"), Accessors.STRING, 16, new ReadOnlyChannels()));

        String record = "record";
        Cursor cursor = sequence.append(record);
        assertThat(cursor, is(new Cursor(0L)));
        assertThat(sequence.get(cursor), is(record));
        assertThat(sequence.next(cursor), is(new Cursor(10L)));
    }

    @Test
    public void emptyList() throws Exception {
        assertThat(sequence.collect(new Cursor(0L), new Cursor(15L)), is(0L));
    }

    @Test
    public void beginEqualEnd() throws Exception {
        linkedPages.add(mockLinkedPage(0L, 4095L));
        assertThat(sequence.collect(new Cursor(15L), new Cursor(15L)), is(0L));
    }

    @Test
    public void beginGreaterThanEnd() throws Exception {
        linkedPages.add(mockLinkedPage(0L, 4095L));
        assertThat(sequence.collect(new Cursor(15L), new Cursor(7L)), is(0L));
    }

    @Test
    public void lessThanMinimizeCollectLength() throws Exception {
        linkedPages.add(mockLinkedPage(0L, 4095L));
        assertThat(sequence.collect(new Cursor(0L), new Cursor(15L)), is(0L));
    }

    @Test
    public void tooSmallToCollect() throws Exception {
        LinkedPage<String> page = mockLinkedPage(0L, 4095L);
        List<LinkedPage<String>> pieces = Collections.singletonList(page);
        doReturn(pieces).when(page).split(new Cursor(0L), new Cursor(32L));
        linkedPages.add(page);
        assertThat(sequence.collect(new Cursor(0L), new Cursor(32L)), is(0L));
    }

    @Test
    public void collectInOneLinkedPage() throws Exception {
        LinkedPage<String> page = mockLinkedPage(0L, 4095L);
        List<LinkedPage<String>> pieces = Arrays.asList(mockLinkedPage(0L, 14L), mockLinkedPage(64L, 4095));
        doReturn(pieces).when(page).split(new Cursor(15L), new Cursor(64L));
        linkedPages.add(page);
        assertThat(sequence.collect(new Cursor(15L), new Cursor(64L)), is(64 - 15L));
    }

    @Test
    public void collectBetweenTwoLinkedPage() throws Exception {
        LinkedPage<String> page0 = mockLinkedPage(0L, 4095L);
        doReturn(mockLinkedPage(0L, 14L)).when(page0).left(new Cursor(15L));
        LinkedPage<String> page1 = mockLinkedPage(4096L, 8191L);
        doReturn(mockLinkedPage(4112L, 8191L)).when(page1).right(new Cursor(4112L));

        linkedPages.add(page0);
        linkedPages.add(page1);

        assertThat(sequence.collect(new Cursor(15L), new Cursor(4112L)), is(4112 - 15L));

    }

    @Test
    public void collectHoleLeft() throws Exception {
        LinkedPage<String> page0 = mockLinkedPage(0L, 4095L);
        doReturn(null).when(page0).left(new Cursor(15L));
        LinkedPage<String> page1 = mockLinkedPage(4096L, 8191L);
        doReturn(mockLinkedPage(4112L, 8191L)).when(page1).right(new Cursor(4112L));

        linkedPages.add(page0);
        linkedPages.add(page1);

        assertThat(sequence.collect(new Cursor(0L), new Cursor(4112L)), is(4112L));
    }

    @Test
    public void endOffsetOutOfListHasOnlyElement() throws Exception {
        LinkedPage<String> appendingLinkedPage = mockLinkedPage(0L, 4096L);
        doReturn(Collections.singletonList(appendingLinkedPage)).when(appendingLinkedPage)
                .split(new Cursor(0L), new Cursor(4096L));
        linkedPages.add(appendingLinkedPage);
        assertThat(sequence.collect(new Cursor(0L), new Cursor(4096L)), is(0L));
    }

    @Test
    public void beginOffsetOutOfListHasOnlyElement() throws Exception {
        LinkedPage<String> appendingLinkedPage = mockLinkedPage(16L, 4064L);
        doReturn(Collections.singletonList(appendingLinkedPage)).when(appendingLinkedPage)
                .split(new Cursor(0L), new Cursor(4096L));
        linkedPages.add(appendingLinkedPage);
        assertThat(sequence.collect(new Cursor(0L), new Cursor(4096L)), is(0L));
    }

    @Test
    public void collectOnlyLeft() throws Exception {
        LinkedPage<String> page0 = mockLinkedPage(0L, 4095L);
        doReturn(mockLinkedPage(0L, 15L)).when(page0).left(new Cursor(15L));
        LinkedPage<String> page1 = mockLinkedPage(4096L, 8191L);
        doReturn(page1).when(page1).right(new Cursor(4096L));

        linkedPages.add(page0);
        linkedPages.add(page1);

        assertThat(sequence.collect(new Cursor(15L), new Cursor(4096L)), is(4096 - 15L));
    }

    private LinkedPage<String> mockLinkedPage(long begin, long end) {
        LinkedPage<String> page0 = mock(LinkedPage.class);
        when(page0.begin()).thenReturn(begin);
        when(page0.length()).thenReturn(end + 1);
        when(page0.compareTo(any(Cursor.class))).thenCallRealMethod();
        return page0;
    }


}
