package com.github.zhongl.ex.rvs;

import com.github.zhongl.ex.util.Entry;
import com.github.zhongl.ex.util.FutureCallbacks;
import com.github.zhongl.ex.util.Nils;
import com.google.common.util.concurrent.FutureCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class EphemeronsTest {
    SecondLevelStore secondLevelStore;
    Ephemerons4T ephemerons4T;
    ExecutorService service = Executors.newFixedThreadPool(4);

    @Before
    public void setUp() throws Exception {
        secondLevelStore = spy(new SecondLevelStore());
        ephemerons4T = new Ephemerons4T();
    }

    @After
    public void tearDown() throws Exception {
        service.shutdownNow();
    }

    @Test
    public void flowControlAndOrdering() throws Exception {

        final CountDownLatch latch = new CountDownLatch(9);
        for (int i = 0; i < 9; i++) {
            final int num = i;
            service.submit(new Runnable() {
                @Override
                public void run() {
                    waitFor(num); // ensure order
                    try {
                        ephemerons4T.add(num, num, FutureCallbacks.<Void>ignore());
                    } catch (Exception e) {
                        e.printStackTrace();  // TODO right
                    }
                    latch.countDown();
                }
            });
        }

        latch.await();

        ArgumentCaptor<Iterator> argumentCaptor = ArgumentCaptor.forClass(Iterator.class);
        verify(secondLevelStore, times(2)).merge(argumentCaptor.capture());


        // verify the ordering
        int i = 0;

        for (Iterator allValue : argumentCaptor.getAllValues()) {
            while (allValue.hasNext()) {
                Object o = allValue.next();
                Entry<Integer, Integer> entry = (Entry<Integer, Integer>) o;
                assertThat(entry.key(), is(i++));
            }
        }

    }

    class SecondLevelStore {
        final Map<Integer, Integer> secondLevel = new ConcurrentSkipListMap<Integer, Integer>();

        public void merge(Iterator<Entry<Integer, Integer>> entries) {
            waitFor(10L); // mock long time flushing
            while (entries.hasNext()) {
                Entry<Integer, Integer> entry = entries.next();
                if (entry.value() != Nils.OBJECT)
                    secondLevel.put(entry.key(), entry.value());

            }
        }

        public Integer get(Integer key) {
            return secondLevel.get(key);
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
        protected Integer getMiss(Integer key) {
            return secondLevelStore.get(key);
        }

        @Override
        protected void requestFlush(Iterator<Entry<Integer, Integer>> entries, FutureCallback<Void> flushedCallback) {
            secondLevelStore.merge(entries);
            flushedCallback.onSuccess(Nils.VOID);
        }
    }
}
