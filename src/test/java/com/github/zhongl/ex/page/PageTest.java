package com.github.zhongl.ex.page;

import org.junit.Test;

import static com.github.zhongl.ex.journal.OverflowCallback.OverflowThrowing;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class PageTest {

    @Test
    public void main() throws Exception {
        Page<Long, String> page = null;


        Group<Long, String> group = page.newGroup();

        group.append(0L, "1");
        group.append(4 + 1L, "2");

        page.commit(group, true, new OverflowThrowing());

        String v = page.get(0L);
    }

}
