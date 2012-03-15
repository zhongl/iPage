# Overview

## Procedure

                 +---Ephemerons----+
    [add|remove] |                 |
    ------------+|  key -> record  |    flush
                 |                 |---------->>>---------+
        get      |  key -> nil     |                      |
    ------------+|                 |                      |
                 +-----------------+                      |
                        |                                 |
                getMiss |      +--------------Storage-------------------------------+
                        |      |       <current>          |          <merging>      |
                        |      |   +----Snapshot----+     |      +----Snapshot----+ |
                        +-->>>-|--+|  Indices       |---->+>----+|  Indices       | |
                        |      |   |  #             |            |  #             | |
                        +-->>>-|--+|  Binder        |---->+>----+|  Binder        | |
                               |   |  ############  |            |  ############  | |
                               |   +----------------+            +----------------+ |
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

- [Page](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/page/Page.java)
- [Binder](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/page/Binder.java)

# Group commit

    write op:                  *   ***    *
    timeline: ---------------------------------------------------------->
                        ^                       ^
                 preivous flush           next flush

## References

- [FileAppender](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/io/FileAppender.java)

# Index

- Sorted by key;
- Binary search on **PRIVATE MODE** MappedByteBuffer;
-

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

            if (isRemoved(c)) continue; // remove this entry
            append(c);
        }

        mergeRestOf(aItr);
        mergeRestOf(bItr);
    }

## References

- [Indices](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/index/Indices.java)
- [Merger](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/index/Merger.java)

# Fault tolerant

## Directory

     HEAD             // Reference to *.s
    -pages
       *.p            // page file
       *.i            // index file
       *.s            // References to *.l and *.i

## References

- [Storage](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/api/Storage.java)
- [Snapshot](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/api/Snapshot.java)

# Reliable

## fsync

    FileChannel#force(boolean)

## Call flush by

- Count
- Elapse

## References

- [CallByCountOrElapse](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/util/CallByCountOrElapse.java)
- [IPage](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/api/IPage.java)

# Defragment

TODO

## References

- [DefragPolicy](https://github.com/zhongl/iPage/blob/master/src/main/java/com/github/zhongl/api/DefragPolicy.java)

# More...

TODO