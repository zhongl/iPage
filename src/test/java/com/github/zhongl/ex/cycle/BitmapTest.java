package com.github.zhongl.ex.cycle;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BitmapTest {

    @Test
    public void usage() throws Exception {
        Bitmap bitmap = new Bitmap(ByteBuffer.allocate(4096));

        bitmap.set(5, 3);

        assertThat(bitmap.nextSetBit(0), is(5));
        assertThat(bitmap.nextClearBit(5), is(8));
        assertThat(bitmap.nextSetBit(8), is(-1));

        bitmap.reset();

        assertThat(bitmap.nextClearBit(0), is(0));
    }
}
