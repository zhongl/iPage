# What is it?

iPage is a java key-value store library, which is designed for message system.

# Features

* Very fast write by sequence appending
* Random reading by index
* \[A\]synchronize invocation
* Flush By write count or time elpase
* Group commit
* Date recovery after crash
* Garbage data collecting (or Defragment)
* Degrade after OOM
* Monitor and control by JMX

## More

Please see [issues](https://github.com/zhongl/iPage/issues?sort=created&direction=desc&state=open&page=1).

# Benchmark

    | Case            |   Times   | Total Eplase | Average Elapse | TPS         | Concurrency |
    |-----------------|-----------|--------------|----------------|-------------|-------------|
    | Random get      | 1,000,000 | 6.009 s      | 6,008   ns     | 166444.74   | 8           |
    | Async add       | 1,000,000 | 24.90 s      | 24,904  ns     | 40154.19    | 16          |
    | Async remove    | 1,000,000 | 32.84 s      | 32,842  ns     | 30448.82    | 16          |
    | Sync add        | 16,384    | 5.142 s      | 313,857 ns     | 3186.31     | 32          |

* Read and write data is 1KB

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

## Run test

    > git clone git@github.com:zhongl/iPage.git
    > cd iPage
    > mvn clean test

## Run benchmark

    > mvn clean test -Dtest=IPageBenchmark -DargLine="-Xmx512m -Xms512m"

## Usage

Please see:

- [IPageTest](https://github.com/zhongl/iPage/blob/master/src/test/java/com/github/zhongl/api/IPageTest.java)

# More

[Design Doc](https://github.com/zhongl/iPage/blob/master/doc/DESIGN.md)