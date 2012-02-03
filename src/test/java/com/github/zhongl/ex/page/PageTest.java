package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.codec.ComposedCodecBuilder;
import com.github.zhongl.ex.codec.LengthCodec;
import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.util.FileTestContext;
import com.google.common.io.Files;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static com.github.zhongl.ex.page.OverflowCallback.THROW_BY_OVERFLOW;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileTestContext {

    private Page page;

    @Test
    public void get() throws Exception {
        dir = testDir("get");
        page = newPage();

        String one = "1";

        Cursor cursor = page.append(one, true, THROW_BY_OVERFLOW);
        assertThat(cursor.<String>get(), is(one));

        page.close();

        assertThat(cursor.<String>get(), is(one));
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterDeleted() throws Exception {
        dir = testDir("getAfterDeleted");
        page = newPage();
        Cursor cursor = page.append("value", true, THROW_BY_OVERFLOW);
        page.file().delete();
        cursor.get();
    }

    @Test
    public void transfer() throws Exception {
        dir = testDir("transfer");
        page = newPage();

        File src = new File(dir, "src");
        Files.write("value".getBytes(), src);

        assertThat(page.transferFrom(src, 0L), is(5L));

        Files.write(new byte[4092], src);

        assertThat(page.transferFrom(src, 0L), is(4091L));

        src.delete();
    }

    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
    }

    private Page newPage() throws IOException {
        Codec codec = ComposedCodecBuilder.compose(new StringCodec())
                                          .with(LengthCodec.class)
                                          .build();
        return new Page(new File(dir, "0"), mock(Number.class), 4096, codec) {
            @Override
            protected Batch newBatch(CursorFactory cursorFactory, int position, int estimateBufferSize) {
                return new DefaultBatch(cursorFactory, position, estimateBufferSize);
            }
        };
    }
}
