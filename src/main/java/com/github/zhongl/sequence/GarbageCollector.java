/*
 * Copyright 2011 zhongl
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

package com.github.zhongl.sequence;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
class GarbageCollector<T> {

    private final LinkedList<LinkedPage<T>> linkedPages;
    private final long minimizeCollectLength;

    public GarbageCollector(LinkedList<LinkedPage<T>> linkedPages, long minimizeCollectLength) {
        this.linkedPages = linkedPages;
        this.minimizeCollectLength = minimizeCollectLength;
    }

    public long collect(Cursor begin, Cursor end) throws IOException {
        if (begin.equals(end) || linkedPages.isEmpty() || distanceBetween(begin, end) < minimizeCollectLength)
            return 0L;

        int indexOfBeginPage = begin.indexIn(linkedPages);
        int indexOfEndPage = end.indexIn(linkedPages);

        indexOfBeginPage = indexOfBeginPage < 0 ? 0 : indexOfBeginPage;
        indexOfEndPage = indexOfEndPage < 0 ? linkedPages.size() - 1 : indexOfEndPage;

        if (indexOfBeginPage == indexOfEndPage) return collectIn(indexOfBeginPage, begin, end);

        /*
        *   |   left    |  between |   right   |
        *   |@@@@@|-----|----------|-----|@@@@@|
        *   |      0    |    1     |     2     |
        */
        return collectRight(indexOfEndPage, end) + collectBetween(indexOfEndPage, indexOfBeginPage) + collectLeft(indexOfBeginPage, begin);
    }

    private long collectRight(int index, Cursor cursor) throws IOException {
        LinkedPage<T> right = linkedPages.get(index);
        LinkedPage<T> newLinkedPage = right.right(cursor);
        if (newLinkedPage == right) return 0L;
        linkedPages.set(index, newLinkedPage);
        return cursor.offset - right.begin();
    }

    private long collectLeft(int index, Cursor cursor) throws IOException {
        LinkedPage<T> left = linkedPages.get(index);
        long collectedLength = left.begin() + left.length() - cursor.offset;
        LinkedPage<T> newLeft = left.left(cursor);
        if (newLeft == null) linkedPages.remove(index);
        else linkedPages.set(index, newLeft);
        return collectedLength;
    }

    private long collectBetween(int indexOfEndLinkedPage, int indexOfBeginLinkedPage) {
        long collectedLength = 0L;
        for (int i = indexOfEndLinkedPage - 1; i > indexOfBeginLinkedPage; i--) {
            LinkedPage<T> page = linkedPages.remove(i);
            collectedLength += page.length();
            page.clear();
        }
        return collectedLength;
    }

    private long collectIn(int indexOfLinkedPage, Cursor begin, Cursor end) throws IOException {
        LinkedPage<T> splittingLinkedPage = linkedPages.get(indexOfLinkedPage);
        List<? extends LinkedPage<T>> pieces = splittingLinkedPage.split(begin, end);
        if (pieces.get(0).equals(splittingLinkedPage)) return 0L; // can't left appending page
        linkedPages.set(indexOfLinkedPage, pieces.get(0));
        linkedPages.add(indexOfLinkedPage + 1, pieces.get(1));
        return distanceBetween(begin, end);
    }

    private int distanceBetween(Cursor begin, Cursor end) {
        return end.distanceTo(begin);
    }

}
