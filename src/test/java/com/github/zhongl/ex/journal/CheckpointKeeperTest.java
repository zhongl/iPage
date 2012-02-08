package com.github.zhongl.ex.journal;

import com.github.zhongl.util.FileTestContext;
import org.junit.Test;

import java.io.File;

import static com.github.zhongl.ex.journal.CheckpointKeeper.SUFFIX;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class CheckpointKeeperTest extends FileTestContext {

    @Test
    public void usage() throws Exception {
        dir = testDir("usage");
        CheckpointKeeper keeper = new CheckpointKeeper(dir);
        assertThat(keeper.last(), is(new Checkpoint(0L)));
        Checkpoint one = new Checkpoint(1L);
        keeper.last(one);
        assertThat(keeper.last(), is(one));
        assertExistFile("1" + SUFFIX);
    }

    @Test
    public void load() throws Exception {
        dir = testDir("load");
        new File(dir, "1" + SUFFIX).createNewFile();
        new File(dir, "7" + SUFFIX).createNewFile();
        CheckpointKeeper keeper = new CheckpointKeeper(dir);
        assertThat(keeper.last(), is(new Checkpoint(7L)));
    }
}
