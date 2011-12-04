package com.github.zhongl.ipage;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@ThreadSafe
public class Md5Key {

    public static final int LENGTH = 16;
    private final byte[] md5Bytes;

    public static Md5Key readFrom(ByteBuffer buffer) {
        byte[] bytes = new byte[LENGTH];
        buffer.get(bytes);
        return new Md5Key(bytes);
    }

    public static Md5Key valueOf(Record record) {
        byte[] bytes = DigestUtils.md5(record.toBytes());
        return new Md5Key(bytes);
    }

    public static Md5Key valueOf(byte[] bytes) {
        return new Md5Key(DigestUtils.md5(bytes));
    }


    public Md5Key(byte[] md5Bytes) {
        checkArgument(md5Bytes.length == LENGTH, "Invalid md5 bytes length %s", md5Bytes.length);
        this.md5Bytes = md5Bytes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Md5Key").append("{md5Bytes=").append(Hex.encodeHexString(md5Bytes)).append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Md5Key md5Key = (Md5Key) o;
        return Arrays.equals(md5Bytes, md5Key.md5Bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(md5Bytes);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(md5Bytes);
    }
}
