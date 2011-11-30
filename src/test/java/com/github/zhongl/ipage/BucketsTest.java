package com.github.zhongl.ipage;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

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
    @Ignore("TODO")
    public void cleanup() throws Exception {
        // TODO cleanup
    }

    private void fillFullBuckets() throws Exception {
        buckets = new Buckets(file, 1);
        for (int i = 0; i < 163; i++) {
            buckets.put(Md5Key.valueOf(Ints.toByteArray(i)), 10L);
        }
    }

}
