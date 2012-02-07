package com.github.zhongl.ex.api;

import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Checkpoint;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.ex.util.Entry;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.Callable;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultRecorderTest {

    private DefaultRecorder recorder;

    @Test
    public void usage() throws Exception {
        Journal journal = mock(Journal.class);
        QuanlityOfService quanlityOfService = mock(QuanlityOfService.class);
        doReturn(mock(Callable.class)).when(quanlityOfService).append(eq(journal), any(Entry.class));

        FlowControllor controllor = spy(new FlowControllor());
        recorder = new DefaultRecorder(journal, quanlityOfService, controllor, 10000, 10000L);

        byte[] value = "value".getBytes();
        Md5Key key = Md5Key.generate(value);

        recorder.append(key, value);
        verify(quanlityOfService).append(journal, new Entry<Md5Key, byte[]>(key, value));
        verify(controllor, times(1)).call(any(Callable.class));

        recorder.remove(key);
        verify(quanlityOfService).append(journal, new Entry<Md5Key, byte[]>(key, DefaultRecorder.NULL_VALUE));
        verify(controllor, times(2)).call(any(Callable.class));

        Checkpoint checkpoint = new Checkpoint(1L);
        recorder.erase(checkpoint);
        Thread.sleep(1L);
        verify(journal).erase(checkpoint);

    }

    @After
    public void tearDown() throws Exception {
        recorder.stop();
    }
}
