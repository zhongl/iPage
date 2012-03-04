# Overview

## Procedure

                 +---Ephemerons----+
    [add|remove] |                 |
    ------------+|  key -> record  |    flush
                 |                 |---------->>>----------+
        get      |  key -> nil     |                       |
    ------------+|                 |                       |
                 +-----------------+                       |
                        |                                  |
                getMiss |      +--------------Storage-------------------------------+
                        |      |                           |                        |
                        |      |   +----Snapshot----+      |      +-----Merge-----+ |
                        +-->>>-|--+|  ReadOnlyIndex |----->+>----+|  IndexMerge   | |
                        |      |   |  ###           |             |  ###          | |
                        +-->>>-|--+|  ReadOnlyLine  |----->+>----+|  LineAppender | |
                               |   |  ############  |             |  ##########   | |
                               |   +----------------+             +---------------+ |
                               |           +--------------<<<--------------+        |
                               |                          swap                      |
                               +----------------------------------------------------+

## Structure

    Record  := (id, key, value, callback)
    Index   := [Item...]
    Item    := (key, Range)
    Range   := (from, to)
    Line    := [Entry...]
    Entry   := (key, length, value)

# Pagination

    offset  0                 1                 2                ...                n
            |-----------------|-----------------|-----------------|-----------------|
            |<---- page1 ---->|<---- page2 ---->|<---- ..... ---->|<---- pageN ---->|
            |<------------------------------- Binder ------------------------------>|

## References

- [Page](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/Page.java)
- [Binder](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/Binder.java)

# Group commit

    write op:                  *   ***    *
    timeline: ---------------------------------------------------------->
                        ^                       ^
                 preivous flush           next flush

## References

- [LineAppender](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/LineAppender.java)

# Index

- Sorted by key;
- Binary search on **PRIVATE MODE** MappedByteBuffer;
- Split files by max size.

## Merging

    public void merge(Iterator<Entry<Key, Range>> base, Iterator<Entry<Key, Range>> delta) {
        PeekingIterator<Entry<Key, Range>> aItr = Iterators.peekingIterator(base);
        PeekingIterator<Entry<Key, Range>> bItr = Iterators.peekingIterator(delta);

        while (aItr.hasNext() && bItr.hasNext()) {

            Entry<Key, Range> a = aItr.peek();
            Entry<Key, Range> b = bItr.peek();
            Entry<Key, Range> c;

            int result = a.compareTo(b);

            if (result < 0) c = aItr.next();      // a <  b, use a
            else if (result > 0) c = bItr.next(); // a >  b, use b
            else {                                // a == b, use b instead a
                c = b;
                bItr.next();
                aItr.next();
            }

            if (remove(c)) continue; // remove this entry
            append(c);
        }

        mergeRestOf(aItr);
        mergeRestOf(bItr);
    }

## References

- [ReadOnlyIndex](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/ReadOnlyIndex.java)
- [IndexMerger](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/IndexMerger.java)

# Fault tolerant

## Directory

     HEAD             // Reference to *.s
    -pages
       *.l            // line file
       *.i            // index file
       *.s            // References to *.l and *.i

## References

- [Storage](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/Storage.java)
- [Snapshot](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/Snapshot.java)
- [TextFile](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/TextFile.java)

# Reliable

## fsync

    FileChannel#force(boolean)

## Call flush by

- Count
- Elapse

## References

- [LineAppender](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/LineAppender.java)
- [CallByCountOrElapse](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/util/CallByCountOrElapse.java)
- [IPage](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/ipage/IPage.java)

# More...

TODO