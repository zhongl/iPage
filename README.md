# What is it?

iPage is a light-weight java key-value store lib for message system.

# Features

* Very fast write by sequence appending
* Random reading by index
* [A]synchronize invocation
* Flush By write count or time elpase
* Group commit
* Date recovery after crash

## Coming soon

Please see [issues](https://github.com/zhongl/iPage/issues?sort=created&direction=desc&state=open&page=1).

# Benchmark

    | Operation(sync) | Times | Total Eplase | Average Elapse | Minimize Elapse | Maximzie Elapse |
    |-----------------|-------|--------------|----------------|-----------------|-----------------|
    | Get             | 1000  | 0ms          | 0ms            | 0ms             | 0ms             |
    | Remove          | 1000  | 497ms        | 0ms            | 0ms             | 12ms            |
    | Put             | 1000  | 2,080ms      | 2ms            | 0ms             | 61ms            |

## Configuration

* Synchronize invocation
* Flush by count 5 or elapse 10ms
* Chunk capacity is 32MB
* Initial bucket size is 100
* Group commit is on

## Enviroment

* Linux 2.6.18-164.el5
* OpenJDK 64-Bit Server VM (build 1.6.0-b09, mixed mode)
* Intel(R) Xeon(R) CPU E5620  @ 2.40GHz 8 core
* Memory 24GB
* SCSI 1TB

# Get started

## Precondition

Suppose you enviroment has:

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

    > mvn clean test -Dtest=KVEngineBenchmark

## Use KVEngine

Here is a simple test case code:

    File dir = new File("/path/to/dir");
    KVEngine engine = KVEngine.baseOn(dir).build();
	engine.startup();
	Record record = new Record("record");
	Md5Key key = Md5Key.valueOf(record);

	assertThat(engine.put(key, record), is(nullValue()));
	assertThat(engine.get(key), is(record));
	assertThat(engine.remove(key), is(record));

More usage please see <https://github.com/zhongl/iPage/tree/master/src/test/java/com/github/zhongl/ipage> .