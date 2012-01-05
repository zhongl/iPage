/*
 * Copyright 2012 zhongl
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.kvengine;

import com.github.zhongl.index.Md5Key;
import com.github.zhongl.journal.Event;
import com.github.zhongl.journal.Events;
import com.github.zhongl.page.Accessor;
import com.google.common.util.concurrent.FutureCallback;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class Md5KeyEvents<T> implements Events<Md5Key, T> {
    private final Accessor<T> accessor;

    public Md5KeyEvents(Accessor<T> accessor) {
        this.accessor = accessor;
    }

    @Override
    public Md5Key getKey(Event event) {
        return ((InnerEvent) event).key;
    }

    @Override
    public T getValue(Event event) {
        return ((InnerEvent) event).value;
    }

    @Override
    public boolean isAdd(Event event) {
        return ((InnerEvent) event).type == EventType.ADD;
    }

    @Override
    public Writer writer(final Event value) {
        final EventType type = ((InnerEvent) value).type;
        final Writer kWriter = Md5Key.ACCESSOR.writer(getKey(value));
        final Writer vWriter = accessor.writer(getValue(value));
        return new Writer() {
            @Override
            public int byteLength() {
                int length = 1 + kWriter.byteLength();
                if (type == EventType.DEL) return length;
                return length + vWriter.byteLength();
            }

            @Override
            public int writeTo(WritableByteChannel channel) throws IOException {
                int wrote = type.writeTo(channel) + kWriter.writeTo(channel);
                if (type == EventType.DEL) return wrote;
                return wrote + vWriter.writeTo(channel);
            }
        };
    }

    @Override
    public Reader<Event> reader() {
        final Reader<Md5Key> kReader = Md5Key.ACCESSOR.reader();
        final Reader<T> vReader = accessor.reader();
        return new Reader<Event>() {
            @Override
            public Event readFrom(ReadableByteChannel channel) throws IOException {
                EventType type = EventType.readFrom(channel);
                return type == EventType.ADD
                        ? new InnerEvent(kReader.readFrom(channel), vReader.readFrom(channel))
                        : new InnerEvent(kReader.readFrom(channel));
            }
        };
    }


    public Event put(Md5Key key, T value, FutureCallback<T> callback, T previous) {
        return new InnerEvent(key, value, callback, previous);
    }

    public Event remove(Md5Key key, FutureCallback<T> callback, T previous) {
        return new InnerEvent(key, callback, previous);
    }

    enum EventType {
        ADD, DEL;

        int writeTo(WritableByteChannel channel) throws IOException {
            return channel.write(ByteBuffer.wrap(new byte[] {(byte) ordinal()}));
        }

        static EventType readFrom(ReadableByteChannel channel) throws IOException {
            byte[] onebyte = new byte[1];
            channel.read(ByteBuffer.wrap(onebyte));
            if (onebyte[0] == ADD.ordinal()) return ADD;
            if (onebyte[0] == DEL.ordinal()) return DEL;
            throw new IllegalStateException("Unknown event type " + onebyte[0]);
        }
    }

    private class InnerEvent implements Event {
        final Md5Key key;
        final T value;
        final EventType type;

        private final FutureCallback<T> callback;
        private final T previous;

        private InnerEvent(Md5Key key, T value, FutureCallback<T> callback, T previous, EventType type) {
            this.key = key;
            this.value = value;
            this.callback = callback;
            this.previous = previous;
            this.type = type;
        }

        public InnerEvent(Md5Key key, T value, FutureCallback<T> callback, T previous) {
            this(key, value, callback, previous, EventType.ADD);
        }

        public InnerEvent(Md5Key key, FutureCallback<T> callback, T previous) {
            this(key, null, callback, previous, EventType.DEL);
        }

        public InnerEvent(Md5Key key, T value) {
            this(key, value, null, null, EventType.ADD);
        }

        public InnerEvent(Md5Key key) {
            this(key, null, null, null, EventType.DEL);
        }

        @Override
        public void onSuccess(Void result) {
            callback.onSuccess(previous);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.onFailure(t);
        }
    }
}
