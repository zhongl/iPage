package com.github.zhongl.ipage;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link Record} is wrapper of bytes, a minimized store unit.
 *
 * @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a>
 */
@ThreadSafe
public class Record {
    public static final int LENGTH_BYTES = 4;
    private final ByteBuffer buffer;

    public static Record readFrom(ByteBuffer byteBuffer) throws IOException {
        int length = byteBuffer.getInt();
        checkArgument(length > 0, "Invalid length %s", length);
        int limit = byteBuffer.position() + length;
        checkArgument(limit <= byteBuffer.limit(), "Half content");
        byteBuffer.limit(limit);
        return new Record(byteBuffer.slice());
    }

    private Record(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public Record(byte[] bytes) {
        buffer = ByteBuffer.wrap(bytes);
    }

    public int length() {
        return buffer.limit();
    }

    public int writeTo(ByteBuffer buffer) throws IOException {
        this.buffer.position(0);
        buffer.putInt(length()).put(this.buffer.duplicate());
        return LENGTH_BYTES + length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return !(buffer != null ? !buffer.equals(record.buffer) : record.buffer != null);
    }

    @Override
    public int hashCode() {
        return buffer != null ? buffer.hashCode() : 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Record");
        sb.append("{bytes=").append(Arrays.toString(toBytes()));
        sb.append('}');
        return sb.toString();
    }

    public byte[] toBytes() {
        if (buffer.isDirect()) {
            byte[] bytes = new byte[buffer.limit()];
            buffer.duplicate().get(bytes);
            return bytes;
        } else {
            return buffer.array();
        }
    }
}
