package com.github.zhongl.ipage;

import com.github.zhongl.util.CallbackFuture;
import com.github.zhongl.util.Entry;
import com.github.zhongl.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class EphemeronsTest {
    SecondLevelStore secondLevelStore;
    Ephemerons4T ephemerons4T;

    @Before
    public void setUp() throws Exception {
        secondLevelStore = spy(new SecondLevelStore());
        ephemerons4T = new Ephemerons4T();
    }

    @Test
    public void flowControlAndOrdering() throws Exception {

        CallbackFuture<Void> future = null;
        for (int i = 0; i < 8; i++) {
            final int num = i;
            try {
                future = new CallbackFuture<Void>();
                ephemerons4T.add(num, num, future);
            } catch (Exception e) {
                e.printStackTrace();  // TODO right
            }
        }

        ephemerons4T.flush();
        future.get();

        ArgumentCaptor<Collection> appendingsCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Collection> removingsCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(secondLevelStore, times(2)).merge(appendingsCaptor.capture(), removingsCaptor.capture());

        // verify the ordering
        int i = 0;

        for (Collection allValue : appendingsCaptor.getAllValues()) {
            for (Object o : allValue) {
                Entry<Integer, Integer> entry = (Entry<Integer, Integer>) o;
                assertThat(entry.key(), is(i++));
            }
        }

    }

    class SecondLevelStore {
        final Map<Integer, Integer> secondLevel = new ConcurrentSkipListMap<Integer, Integer>();

        public Integer get(Integer key) {
            return secondLevel.get(key);
        }

        public void merge(Collection<Entry<Integer, Integer>> appendings, Collection<Integer> removings) {
            waitFor(10L); // mock long time flushing
            for (Entry<Integer, Integer> entry : appendings) {
                if (entry.value() != Nils.OBJECT)
                    secondLevel.put(entry.key(), entry.value());
            }
            for (Integer key : removings) {
                secondLevel.remove(key);
            }
        }
    }

    private void waitFor(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class Ephemerons4T extends Ephemerons<Integer, Integer> {

        protected Ephemerons4T() {
            super(new ConcurrentHashMap<Integer, Record>());
            throughout(4);
        }

        @Override
        protected void requestFlush(Collection<Entry<Integer, Integer>> appendings, Collection<Integer> removings, FutureCallback<Void> flushedCallback) {
            secondLevelStore.merge(appendings, removings);
            flushedCallback.onSuccess(Nils.VOID);
        }

        @Override
        protected Integer getMiss(Integer key) {
            return secondLevelStore.get(key);
        }

    }
}
