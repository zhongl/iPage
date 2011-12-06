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

import com.github.zhongl.util.FileBase;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import static com.github.zhongl.ipage.Recovery.RecordFinder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BucketsTest extends FileBase {

    private Buckets buckets;

    @Override
    public void tearDown() throws Exception {
        if (buckets != null) buckets.close();
        super.tearDown();
    }

    @Test
    public void useDefaultSizeIfInvalidBucketSizeWereSet() throws Exception {
        file = testFile("invalidBucketSize");
        assertThat(new Buckets(file, -1).size(), is(256));
    }

    @Test
    public void putAndGet() throws Exception {
        file = testFile("putAndGet");
        buckets = new Buckets(file, 1);

        Md5Key key = Md5Key.valueOf("key".getBytes());

        long offset = 7L;
        assertThat(buckets.put(key, offset), is(nullValue()));
        assertThat(buckets.get(key), is(offset));

        buckets.close();

        buckets = new Buckets(file, 1);
        assertThat(buckets.get(key), is(offset)); // assert sync to file
    }

    @Test
    public void remove() throws Exception {
        file = testFile("remove");
        buckets = new Buckets(file, 1);

        Md5Key key = Md5Key.valueOf("key".getBytes());
        Long offset = 7L;
        assertThat(buckets.put(key, offset), is(nullValue()));
        assertThat(buckets.remove(key), is(offset));
        assertThat(buckets.get(key), is(nullValue()));
    }

    @Test
    public void validateWithoutRecovery() throws Exception {
        file = testFile("validateAndRecoverBy");

        buckets = new Buckets(file, 1);
        RecordFinder recordFinder = mock(RecordFinder.class);
        assertThat(buckets.validateAndRecoverBy(recordFinder), is(false));// validateAndRecoverBy a empty bucket

        buckets.put(Md5Key.valueOf("key".getBytes()), 7L);
        buckets.close();
        buckets = new Buckets(file, 1);
        doReturn(new Record("key".getBytes())).when(recordFinder).getRecordIn(anyLong());
        assertThat(buckets.validateAndRecoverBy(recordFinder), is(false)); // validateAndRecoverBy a exist bucket
    }

    @Test
    public void validateWithRecovery() throws Exception {
        file = testFile("validateWithRecovery");
        byte[] md5Bytes0 = DigestUtils.md5("value0");
        byte[] md5Bytes1 = DigestUtils.md5("value1");
        byte[] bucketMd5 = DigestUtils.md5("bucket");
        byte[] brokenBucketContent = Bytes.concat(
                new byte[] {1},
                md5Bytes0,
                Longs.toByteArray(4L),
                new byte[] {1},
                md5Bytes1,
                Longs.toByteArray(7L),
                new byte[4055],
                bucketMd5
        );
        Files.write(brokenBucketContent, file); // mock broken buckets file.

        buckets = new Buckets(file, 1);

        RecordFinder recordFinder = mock(RecordFinder.class);
        doReturn(new Record("value0".getBytes())).when(recordFinder).getRecordIn(4L);
        doReturn(new Record("broken".getBytes())).when(recordFinder).getRecordIn(7L); // broken index
        assertThat(buckets.validateAndRecoverBy(recordFinder), is(true));
    }

    @Test
    public void putInReleasedSlot() throws Exception {
        file = testFile("putInReleasedSlot");
        buckets = new Buckets(file, 1);

        fillFullBuckets();

        Md5Key key0 = Md5Key.valueOf(Ints.toByteArray(0));
        Md5Key key1 = Md5Key.valueOf(Ints.toByteArray(1));
        buckets.remove(key0);
        buckets.remove(key1);
        assertThat(buckets.put(key1, 7L), is(nullValue()));
        assertThat(buckets.put(key0, 7L), is(nullValue())); // no exception means new item index put in released slot.
    }

    @Test
    public void getAndRemoveEmplyBucket() throws Exception {
        file = testFile("getAndRemoveEmplyBucket");
        buckets = new Buckets(file, 1);

        Md5Key key = Md5Key.valueOf(Ints.toByteArray(1));
        assertThat(buckets.get(key), is(nullValue()));
        assertThat(buckets.remove(key), is(nullValue()));
    }

    @Test
    public void getAndRemoveByInvalidKey() throws Exception {
        file = testFile("getAndRemoveByInvalidKey");
        buckets = new Buckets(file, 1);

        fillFullBuckets();

        Md5Key invalidKey = Md5Key.valueOf(Ints.toByteArray(163));

        assertThat(buckets.get(invalidKey), is(nullValue()));
        assertThat(buckets.remove(invalidKey), is(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void unknownSlotState() throws Exception {
        file = testFile("unknownSlotState");
        buckets = new Buckets(file, 1);
        Files.write(new byte[] {3}, file);  // write a unknown slot state
        buckets.get(Md5Key.valueOf(Ints.toByteArray(1))); // trigger exception
    }

    @Test(expected = OverflowException.class)
    public void noSlotForNewItemIndex() throws Exception {
        file = testFile("noSlotForNewItemIndex");
        fillFullBuckets();
        Md5Key key163 = Md5Key.valueOf(Ints.toByteArray(163));
        buckets.put(key163, 7L);  // trigger exception
    }

    @Test
    public void replace() throws Exception {
        file = testFile("replace");
        buckets = new Buckets(file, 1);
        Md5Key key = Md5Key.valueOf("key".getBytes());
        assertThat(buckets.put(key, 7L), is(nullValue()));
        assertThat(buckets.put(key, 4L), is(7L));
    }

    @Test
    public void cleanup() throws Exception {
        file = testFile("cleanup");
        buckets = new Buckets(file, 1);
        Md5Key key = Md5Key.valueOf("key".getBytes());
        buckets.remove(key); // test remove a non-exist key with no effect
        buckets.put(key, 7L);
        buckets.put(key, 7L);
        buckets.remove(key);
        buckets.cleanupIfAllKeyRemoved();
        assertThat(file.exists(), is(false));
    }

    private void fillFullBuckets() throws Exception {
        buckets = new Buckets(file, 1);
        for (int i = 0; i < 163; i++) {
            buckets.put(Md5Key.valueOf(Ints.toByteArray(i)), 10L);
        }
    }

}
