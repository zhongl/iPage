# What is it?

iPage is a key-value store library for java (or any others base on JVM).

# Features

* Very fast write by sequence appending
* Random reading by index
* \[A\]synchronize invocation
* Flush By write count or time elpase
* Group commit
* Date recovery after crash
* \[Auto\] garbage data collecting

## More

Please see [issues](https://github.com/zhongl/iPage/issues?sort=created&direction=desc&state=open&page=1).

# Benchmark

    | Operation(sync) |   Times   | Total Eplase | Average Elapse | Minimize Elapse | Maximzie Elapse |
    |-----------------|-----------|--------------|----------------|-----------------|-----------------|
    | Get             | 1,000     | 36ms         | 0ms            | 0ms             | 1ms             |
    | Remove          | 1,000     | 97ms         | 0ms            | 0ms             | 11ms            |
    | Put             | 1,000     | 1,583ms      | 1ms            | 0ms             | 28ms            |
    |-----------------|-----------|--------------|----------------|-----------------|-----------------|
    | Get             | 1,000,000 | 9,850ms      | 0ms            | 0ms             | 25ms            |
    | Remove          | 1,000,000 | 32,421ms     | 0ms            | 0ms             | 74ms            |
    | Put             | 1,000,000 | 1,111,221ms  | 0ms            | 0ms             | 257ms           |

## Configuration

* Concurrency threads is 8
* Read and write data is 1KB
* Synchronize invocation
* Flush by count 4 or elapse 10ms
* Chunk capacity is 128MB
* Initial bucket size is 8192
* Group commit is on

## Enviroment

* Linux 2.6.18-164.el5
* OpenJDK 64-Bit Server VM (build 1.6.0-b09, mixed mode)
* Intel(R) Xeon(R) CPU E5620  @ 2.40GHz 8 core
* Memory 24GB
* SCSI 1TB

# Getting started

## Preconditions

Make sure your enviroment has:

* [Git](http://git-scm.com/)
* [Maven](http://maven.apache.org/)

## Install [Benchmarker](https://github.com/zhongl/Benchmarker)

iPage need it to run benchmark:

    > git clone git@github.com:zhongl/Benchmarker.git
    > cd Benchmarker
    > mvn clean install

## Run test

    > git clone git@github.com:zhongl/iPage.git
    > cd iPage
    > mvn clean test

## Run benchmark

    > mvn clean test -Dtest=BlockingKVEngineBenchmark -Dblocking.kvengine.benchmark.times=1000

more about `-D` options you can find in [BlockingKVEngineBenchmark.java](https://github.com/zhongl/iPage/blob/master/src/test/java/com/github/zhongl/kvengine/BlockingKVEngineBenchmark.java).


# Documents

Please see the [wiki](http://https://github.com/zhongl/iPage/wiki).