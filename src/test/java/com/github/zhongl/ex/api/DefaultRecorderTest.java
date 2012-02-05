package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultRecorderTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        DefaultRecorder recorder = new DefaultRecorder(dir, QuanlityOfService.RELIABLE);

        byte[] value = "value".getBytes();
        Md5Key key = Md5Key.generate(value);

        recorder.append(key, value);

        recorder.remove(key);
    }
}
