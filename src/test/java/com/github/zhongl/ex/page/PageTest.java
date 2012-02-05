package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.codec.ComposedCodecBuilder;
import com.github.zhongl.ex.codec.LengthCodec;
import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.util.CallbackFuture;
import com.github.zhongl.util.FileTestContext;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest extends FileTestContext {

    private Page page;

    @Test
    public void get() throws Exception {
        dir = testDir("get");
        page = newPage();

        final String one = "1";

        page.append(one, new FutureCallback<Cursor>() {
            @Override
            public void onSuccess(Cursor cursor) {
                assertThat(cursor.<String>get(), is(one));
            }

            @Override
            public void onFailure(Throwable t) {
                fail();
            }
        });

        page.force();
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterDeleted() throws Exception {
        dir = testDir("getAfterDeleted");
        page = newPage();
        CallbackFuture<Cursor> callbackFuture = new CallbackFuture<Cursor>();
        page.append("value", callbackFuture);
        page.file().delete();
        callbackFuture.get().get();
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
