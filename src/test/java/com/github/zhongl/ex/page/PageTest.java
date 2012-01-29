package com.github.zhongl.ex.page;

import org.junit.After;
import org.junit.Test;

import static com.github.zhongl.ex.journal.OverflowCallback.OverflowThrowing;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest {

    private Page page;

    protected Page newPage() {
        return null;  // TODO newPage
    }

    @Test
    public void get() throws Exception {
        page = newPage();

        Group group = page.newGroup();
        String one = "1";

        Cursor<String> cursor = group.append(one);
        assertThat(cursor.get(), is(one));

        page.commit(group, true, new OverflowThrowing());
        assertThat(cursor.get(), is(one));

        page.close();

        assertThat(cursor.get(), is(one));
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterDeleted() throws Exception {
        page = newPage();

        Group group = page.newGroup();
        Cursor<String> cursor = group.append("value");
        page.commit(group, true, new OverflowThrowing());

        page.delete();

        cursor.get();
    }

    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
    }
}
