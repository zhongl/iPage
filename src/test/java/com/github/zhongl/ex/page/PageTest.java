package com.github.zhongl.ex.page;

import com.github.zhongl.util.FileTestContext;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public abstract class PageTest extends FileTestContext {

    private Page page;

    protected abstract Page newPage() throws IOException;

    @Test
    public void get() throws Exception {
        page = newPage();

        Group group = page.newGroup();
        String one = "1";

        Cursor<String> cursor = group.append(one);
        assertThat(cursor.get(), is(one));

        page.commit(group, true, new OverflowCallback.OverflowThrowing());
        assertThat(cursor.get(), is(one));

        page.close();

        assertThat(cursor.get(), is(one));
    }

    @Test(expected = IllegalStateException.class)
    public void getAfterDeleted() throws Exception {
        page = newPage();

        Group group = page.newGroup();
        Cursor<String> cursor = group.append("value");
        page.commit(group, true, new OverflowCallback.OverflowThrowing());

        page.delete();

        cursor.get();
    }

    @After
    public void tearDown() throws Exception {
        if (page != null) page.close();
    }
}
