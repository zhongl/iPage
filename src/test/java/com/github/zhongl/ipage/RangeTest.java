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

package com.github.zhongl.ipage;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.github.zhongl.ipage.Range.binarySearch;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RangeTest {

    @Test
    public void search() throws Exception {
        List<Range> rangeList = Arrays.asList(new Range(1, 3), new Range(4, 7), new Range(8, 9));
        assertThat(binarySearch(rangeList, 10), lessThan(0));
        assertThat(binarySearch(rangeList, 9), is(2));
        assertThat(binarySearch(rangeList, 8), is(2));
        assertThat(binarySearch(rangeList, 7), is(1));
        assertThat(binarySearch(rangeList, 6), is(1));
        assertThat(binarySearch(rangeList, 4), is(1));
        assertThat(binarySearch(rangeList, 3), is(0));
        assertThat(binarySearch(rangeList, 2), is(0));
        assertThat(binarySearch(rangeList, 1), is(0));
        assertThat(binarySearch(rangeList, 0), lessThan(0));
    }
}
