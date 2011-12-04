package com.github.zhongl.ipage;

import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class RecoveryTest {

    @Test(expected = IllegalStateException.class)
    public void runFailureBecauseOfIOException() throws Exception {
        Index index = mock(Index.class);
        IPage iPage = mock(IPage.class);
        doThrow(new IOException()).when(index).recoverBy(any(Recovery.RecordFinder.class));
        new Recovery(index, iPage).run();
    }

}
