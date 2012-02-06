package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultRecorderTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        Journal journal = spy(new Journal(dir));
        QuanlityOfService quanlityOfService = spy(QuanlityOfService.RELIABLE);
        FlowControllor controllor = spy(new FlowControllor());
        DefaultRecorder recorder = new DefaultRecorder(journal, quanlityOfService, controllor);

        byte[] value = "value".getBytes();
        Md5Key key = Md5Key.generate(value);

        recorder.append(key, value);

        verify(controllor, times(1)).call(any(Callable.class));

        recorder.remove(key);

        verify(controllor, times(2)).call(any(Callable.class));
    }
}
