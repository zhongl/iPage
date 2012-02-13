package com.github.zhongl.ipage;

import com.github.zhongl.util.*;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class StorageTest extends FileTestContext {


    @Test
    public void usage() throws Exception {
        dir = testDir("usage");

        String value = "value";
        Key key = new Key(Md5.md5(value.getBytes()));

        final Storage<String> storage = new Storage<String>(dir, new StringCodec());
        final Collection<Entry<Key, String>> appendings = Arrays.asList(new Entry<Key, String>(key, value));
        final Collection<Key> removings = Arrays.asList();

        FutureCallbacks.call(new Function<FutureCallback<Void>, Void>() {
            @Override
            public Void apply(@Nullable FutureCallback<Void> callback) {
                storage.merge(appendings, removings, callback);
                return Nils.VOID;
            }
        });

        assertThat(storage.get(key), is(value));
    }

    private static class StringCodec implements Codec<String> {
        @Override
        public String decode(ByteBuffer buffer) {
            int length = buffer.limit() - buffer.position();
            byte[] bytes = new byte[length];
            buffer.get(bytes);
            return new String(bytes);
        }

        @Override
        public ByteBuffer encode(String object) {
            return ByteBuffer.wrap(object.getBytes());
        }
    }
}
