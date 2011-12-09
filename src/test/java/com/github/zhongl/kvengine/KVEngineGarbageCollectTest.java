package com.github.zhongl.kvengine;

import com.github.zhongl.accessor.CommonAccessors;
import com.github.zhongl.index.Md5Key;
import com.google.common.primitives.Ints;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl</a>
 */
public class KVEngineGarbageCollectTest extends KVEngineBase {

    @Test
    public void garbageCollect() throws Exception {
        dir = testDir("garbageCollect");
        engine = KVEngine.<String>baseOn(dir).valueAccessor(CommonAccessors.STRING).build();
        engine.startup();

        String value = "0123456789ab";
        for (int i = 0; i < 257; i++) {
            engine.put(Md5Key.generate(Ints.toByteArray(i)), value);
        }
        for (int i = 0; i < 256; i++) {
            engine.remove(Md5Key.generate(Ints.toByteArray(i)));
        }
        long collectedLength = engine.garbageCollect();
        assertThat(collectedLength, is(8192L));
    }
}
