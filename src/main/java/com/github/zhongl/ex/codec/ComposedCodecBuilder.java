package com.github.zhongl.ex.codec;

import java.util.ArrayList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class ComposedCodecBuilder {

    private final Codec baseCodec;
    private final List<Class<? extends DecoratedCodec>> decorators;

    public static ComposedCodecBuilder compose(Codec codec) {
        return new ComposedCodecBuilder(codec);
    }

    private ComposedCodecBuilder(Codec codec) {
        this.baseCodec = codec;
        this.decorators = new ArrayList<Class<? extends DecoratedCodec>>();
    }

    public ComposedCodecBuilder with(Class<? extends DecoratedCodec> decorator) {
        this.decorators.add(decorator);
        return this;
    }

    public Codec build() {
        Codec codec = baseCodec;
        try {
            for (Class<? extends DecoratedCodec> decorator : decorators) {
                codec = decorator.getConstructor(Codec.class).newInstance(codec);
            }
        } catch (Exception e) {
            throw new RuntimeException(e); // should not be here
        }
        return codec;
    }
}
