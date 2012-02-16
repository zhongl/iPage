# What is it?

iPage is a java key-value store library, which is designed for message system.

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

    | Case            |   Times   | Total Eplase | Average Elapse | TPS         | Concurrency |
    |-----------------|-----------|--------------|----------------|-------------|-------------|
    | Random get      | 1,000,000 | 7.034 s      | 7,033ns        | 142,186.83  | 8           |
    | Async remove    | 1,000,000 | 23.04 s      | 23,044ns       | 43,395.24   | 2           |
    | Async add       | 1,000,000 | 18.18 s      | 18,180ns       | 55,005.5    | 2           |
    | Sync add        | 16,384    | 4.820 s      | 294,214ns      | 3,399.17    | 32          |

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
