package com.github.zhongl.ex.page;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.codec.ComposedCodecBuilder;
import com.github.zhongl.ex.codec.LengthCodec;
import com.github.zhongl.ex.codec.StringCodec;
import com.github.zhongl.util.FileTestContext;
import org.junit.After;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;

import static com.github.zhongl.util.FileAsserter.assertExist;
import static com.github.zhongl.util.FileAsserter.length;
import static com.github.zhongl.util.FileAsserter.string;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class BinderTest extends FileTestContext {

    private Binder binder;

    @Test
    public void append() throws Exception {
        dir = testDir("append");
        final Codec codec = ComposedCodecBuilder.compose(new StringCodec())
                                                .with(LengthCodec.class)
                                                .build();
        binder = new Binder(dir) {
            @Override
            protected Page newPage(File file, long number) {
                return new Page(file, number, 4096, codec) {
                    @Override
                    protected Batch newBatch(File file, Codec codec) {
                        return new DefaultBatch(file, codec);
                    }
                };
            }

            @Override
            protected long newPageNumber(@Nullable Page last) {
                return last == null ? 0L : last.number() + last.file().length();
            }
        };

        String value = "value";

        Cursor<String> cursor = binder.append(value, true);
        assertThat(cursor.get(), is(value));
        assertExist(new File(dir, "0")).contentIs(length(5), string(value));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (binder != null) binder.close();
        super.tearDown();
    }
}
