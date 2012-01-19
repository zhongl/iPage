package com.github.zhongl.journal1;

import com.google.common.primitives.Ints;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CodecsTest {

    @Test
    public void register() throws Exception {
        Codecs codecs = new Codecs(new KeyCodec());
        Key instance = new Key(1);
        ByteBuffer buffer = codecs.encode(instance);
        assertThat(codecs.<Key>decodeAndValidate(buffer), is(instance));
    }

    class Key {
        int i;

        Key(int i) { this.i = i; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return i == key.i;
        }

        @Override
        public int hashCode() {
            return i;
        }

        @Override
        public String toString() {
            return "Key{" +
                    "i=" + i +
                    '}';
        }
    }

    class KeyCodec implements Codec {

        @Override
        public ByteBuffer encode(Object instance) {
            Key key = (Key) instance;
            return ByteBuffer.wrap(Ints.toByteArray(key.i));
        }

        @Override
        public Key decode(ByteBuffer buffer) {
            return new Key(buffer.getInt(0));
        }

        @Override
        public Class<?> supported() {
            return Key.class;
        }
    }
}
