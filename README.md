# go-ycsb 

go-ycsb is a Go port of [YCSB](https://github.com/brianfrankcooper/YCSB). It fully supports all YCSB generators and the Core workload so we can do the basic CRUD benchmarks with Go.

## Getting Started

```bash
git clone https://github.com/pingcap/go-ycsb.git $GOPATH/src/github.com/pingcap/go-ycsb
cd $GOPATH/src/github.com/pingcap/go-ycsb
make

./bin/go-ycsb
```

Notice:

+ To use FoundationDB, you must install [client](https://www.foundationdb.org/download/) library at first.

## Usage 

Mostly, we can start from the offical document [Running-a-Workload](https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload).

### Shell

```basic
./bin/go-ycsb shell basic
» help
YCSB shell command

Usage:
  shell [command]

Available Commands:
  delete      Delete a record
  help        Help about any command
  insert      Insert a record
  read        Read a record
  scan        Scan starting at key
  table       Get or [set] the name of the table
  update      Update a record
```

### Load

```bash
./bin/go-ycsb load basic -P workloads/workloada
```

### Run

```bash
./bin/go-ycsb run basic -P workloads/workloada
```

## Supported Database

- basic
- mysql
- tikv
- foundationdb or fdb

## Database Configuration

You can pass the database configuraitons through `-p field=value` in the command line directly.

### FoundationDB

|field|default value|description|
|-|-|-|
|fdb.cluster|""|The cluster file used for FoundationDB, if not set, will use the [default](https://apple.github.io/foundationdb/administration.html#default-cluster-file)|
|fdb.dbname|"DB"|The cluster database name|
|fdb.apiversion|510|API version, now only 5.1 is supported|

## TODO

- [ ] Support more measurement, like HdrHistogram
- [ ] Add tests for generators
