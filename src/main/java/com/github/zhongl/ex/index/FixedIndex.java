package com.github.zhongl.ex.index;

import com.github.zhongl.ex.codec.Codec;
import com.github.zhongl.ex.lang.Entry;
import com.github.zhongl.ex.page.*;
import com.github.zhongl.ex.page.Number;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class FixedIndex extends Index {

    static final String PARTITION_NUM = System.getProperty("ipage.fixed.index.partition.num", "128");

    private static final BigInteger INTERVAL = Md5Key.MAX.bigInteger.divide(new BigInteger(PARTITION_NUM, 10));

    public FixedIndex(File dir) throws IOException { super(dir); }

    @Override
    protected Snapshot newSnapshot(File file, Codec codec) throws IOException {
        return new InnerSnapshot(file, codec);
    }

    private class InnerSnapshot extends Snapshot {

        public InnerSnapshot(File file, Codec codec) throws IOException {
            super(file, codec);
            int partitionNums = Integer.parseInt(PARTITION_NUM);
            if (pages.size() == 1) {
                for (int i = 0; i < partitionNums - 1; i++) pages.add(newPage(last()));
            } else {
                checkArgument(pages.size() == partitionNums, "Invalid fixed index snapshot.");
            }
        }

        @Override
        protected int capacity() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected Number newNumber(@Nullable Page last) {
            if (last == null) return Md5Key.MIN;
            Md5Key number = (Md5Key) parseNumber(last.file().getName());
            BigInteger bigInteger = number.bigInteger.add(INTERVAL);
            return new Md5Key(bigInteger);
        }

        @Override
        public boolean isEmpty() {
            for (Page page : pages)
                if (page.file().length() > 0) return false;
            return true;
        }

        @Override
        protected InnerSnapshot newSnapshotOn(File dir) throws IOException {
            return new InnerSnapshot(dir, codec);
        }

        @Override
        protected void merge(Iterator<Entry<Md5Key, Offset>> sortedIterator, Snapshot snapshot) {
            // TODO
        }
    }
}
