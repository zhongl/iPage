package com.github.zhongl.ex.api;

import com.github.zhongl.ex.actor.Actor;
import com.github.zhongl.ex.index.Md5Key;
import com.github.zhongl.ex.journal.Journal;
import com.github.zhongl.ex.journal.Revision;
import com.github.zhongl.ex.util.Entry;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class QuanlityOfServiceTest {

    private Entry<Md5Key, byte[]> entry;
    private Journal journal;
    private UpdatableSpy updatableSpy;
    private Revision revision;
    private long onSuccessed;

    @Before
    public void setUp() throws Exception {
        entry = mock(Entry.class);
        journal = mock(Journal.class);
        updatableSpy = new UpdatableSpy();
        revision = new Revision(1L);

        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                FutureCallback<Revision> callback = (FutureCallback<Revision>) invocation.getArguments()[1];
                onSuccessed = System.nanoTime();
                callback.onSuccess(revision);
                return null;
            }
        }).when(journal).append(eq(entry), false, (FutureCallback<Revision>) anyObject());
    }

    @Test
    public void reliable() throws Exception {
        QuanlityOfService.RELIABLE.append(journal, entry).call();
        assertThat(updatableSpy.updateRevisionEntry, is(greaterThan(0L))); // invoked
        assertThat(updatableSpy.updateEntry, is(0L)); // not invoked
    }

    @Test
    public void latency() throws Exception {
        QuanlityOfService.LATENCY.append(journal, entry).call();
        assertThat(updatableSpy.updateEntry, is(lessThan(onSuccessed)));
        assertThat(updatableSpy.updateRevisionEntry, is(greaterThan(onSuccessed)));
    }

    @Test
    public void balance() throws Exception {
        QuanlityOfService.BALANCE.append(journal, entry).call();
        assertThat(updatableSpy.updateEntry, is(greaterThan(onSuccessed)));
        assertThat(updatableSpy.updateRevisionEntry, is(greaterThan(updatableSpy.updateEntry)));
    }

    private class UpdatableSpy extends Actor implements Updatable {

        private long updateRevisionEntry;
        private long updateEntry;

        @Override
        public void update(Entry<Md5Key, byte[]> entry) {
            updateEntry = System.nanoTime();
        }

        @Override
        public void update(Revision revision, Entry<Md5Key, byte[]> entry) {
            updateRevisionEntry = System.nanoTime();
        }
    }
}
