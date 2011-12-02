package com.github.zhongl.ipage;

import com.github.zhongl.util.DirBase;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static com.github.zhongl.ipage.ChunkContentUtils.concatToChunkContentWith;
import static com.github.zhongl.ipage.RecordTest.item;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class KVEngineTest extends DirBase {
    private KVEngine engine;

    @After
    public void tearDown() throws Exception {
        if (engine != null) {
            engine.shutdown();
            engine.awaitForShutdown(Integer.MAX_VALUE);
        }
    }

    @Test
    public void appendAndget() throws Exception {
        dir = testDir("appendAndget");
        engine = KVEngine.baseOn(dir).build();
        engine.startup();

        Record record = item("record");
        Md5Key key = Md5Key.valueOf(record);

        AssertFutureCallback<Record> md5KeyCallback = new AssertFutureCallback<Record>();
        AssertFutureCallback<Record> itemCallback = new AssertFutureCallback<Record>();

        engine.put(Md5Key.valueOf(record), record, md5KeyCallback);
        md5KeyCallback.assertResult(is(nullValue(Record.class)));

        engine.get(key, itemCallback);
        itemCallback.assertResult(is(record));
    }

    @Test
    public void flushByInterval() throws Exception {
        dir = testDir("flushByInterval");
        File indexFile = new File(new File(dir, KVEngine.INDEX_DIR), "0");
        File iPageFile = new File(new File(dir, KVEngine.IPAGE_DIR), "0");

        engine = KVEngine.baseOn(dir)
                .initBucketSize(1)
                .flushByCount(1000)
                .flushByIntervalMilliseconds(100L)
                .build();
        engine.startup();


        byte[] bytes = "record".getBytes();
        Record record = new Record(bytes);
        engine.put(Md5Key.valueOf(record), record);

        Thread.sleep(100L);

        byte[] indexContent = Bytes.concat(new byte[] {1}, md5KeyBytesOf(record), Longs.toByteArray(0L));
        FileContentAsserter.of(indexFile).assertIs(indexContent);
        FileContentAsserter.of(iPageFile).assertIs(concatToChunkContentWith(bytes));
    }

    @Test
    public void flushByCount() throws Exception {
        dir = testDir("flushByInterval");
        File indexFile = new File(new File(dir, KVEngine.INDEX_DIR), "0");
        File iPageFile = new File(new File(dir, KVEngine.IPAGE_DIR), "0");

        engine = KVEngine.baseOn(dir)
                .initBucketSize(1)
                .flushByCount(2)
                .flushByIntervalMilliseconds(1000L)
                .build();
        engine.startup();


        byte[] bytes1 = "record1".getBytes();
        byte[] bytes2 = "record2".getBytes();
        Record record1 = new Record(bytes1);
        Record record2 = new Record(bytes2);

        engine.put(Md5Key.valueOf(record1), record1);
        engine.put(Md5Key.valueOf(record2), record2);

        byte[] indexContent = Bytes.concat(
                new byte[] {1}, md5KeyBytesOf(record1), Longs.toByteArray(0L),
                new byte[] {1}, md5KeyBytesOf(record2), Longs.toByteArray(11L)
        );
        FileContentAsserter.of(indexFile).assertIs(indexContent);
        FileContentAsserter.of(iPageFile).assertIs(concatToChunkContentWith(bytes1, bytes2));

        // TODO flushByCount
    }

    private byte[] md5KeyBytesOf(Record record) {
        byte[] md5bytes = new byte[16];
        Md5Key.valueOf(record).writeTo(ByteBuffer.wrap(md5bytes));
        return md5bytes;
    }

}
