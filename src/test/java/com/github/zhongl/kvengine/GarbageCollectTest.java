package com.github.zhongl.kvengine;

import com.github.zhongl.buffer.CommonAccessors;
import com.github.zhongl.index.Md5Key;
import com.github.zhongl.util.FileBase;
import com.google.common.primitives.Ints;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a> */
public class GarbageCollectTest extends FileBase {

    @Test
    public void garbageCollect() throws Exception {
        dir = testDir("garbageCollect");
        BlockingKVEngine<String> engine = new BlockingKVEngine<String>(
                KVEngine.<String>baseOn(dir)
                        .valueAccessor(CommonAccessors.STRING)
                        .build()
        );
        engine.startup();

        String value = "0123456789ab";
        for (int i = 0; i < 257; i++) {
            engine.put(Md5Key.generate(Ints.toByteArray(i)), value);
        }

        for (int i = 0; i < 256; i++) {
            engine.remove(Md5Key.generate(Ints.toByteArray(i))); // remove 0 - 8192
        }
        long collectedLength = engine.garbageCollect(); // plan collect 0 - 8192
        assertThat(collectedLength, is(4096L)); // but last chunk can not collect and minimize collect length is 4096
    }
}
