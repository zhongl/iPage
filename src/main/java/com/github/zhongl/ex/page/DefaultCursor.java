/*
 * Copyright 2012 zhongl                                                                                                                              
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

package com.github.zhongl.ex.page;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
public class DefaultCursor implements Cursor, Comparable<DefaultCursor> {
    private final long offset;
    private final int length;

    public DefaultCursor(long offset, int length) {
        this.offset = offset;
        this.length = length;
    }

    public long offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    @Override
    public String toString() {
        return "Cursor{" +
                "offset=" + offset +
                ", length=" + length +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultCursor cursor = (DefaultCursor) o;
        return length == cursor.length && offset == cursor.offset;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + length;
        return result;
    }

    @Override
    public int compareTo(DefaultCursor o) {
        return offset > o.offset() ? 1 : offset < o.offset ? -1 : 0;
    }
}
