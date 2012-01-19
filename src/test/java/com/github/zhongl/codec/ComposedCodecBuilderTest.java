package com.github.zhongl.codec;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ComposedCodecBuilderTest {

    @Test
    public void usage() throws Exception {
        Codec codec = ComposedCodecBuilder.compose(new CompoundCodec(new StringCodec()))
                .with(LengthCodec.class)
                .with(ChecksumCodec.class)
                .build();

        String value = "value";
        assertThat(codec.<String>decode(codec.encode(value)), is(value));

        assertThat(codec, is(ChecksumCodec.class));
        codec = ((DecoratedCodec) codec).getDelegate();
        assertThat(codec, is(LengthCodec.class));
        codec = ((DecoratedCodec) codec).getDelegate();
        assertThat(codec, is(CompoundCodec.class));
    }

}
