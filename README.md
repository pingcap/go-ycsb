# go-ycsb 

go-ycsb is a Go port of [YCSB](https://github.com/brianfrankcooper/YCSB). It fully supports all YCSB generators and the Core workload so we can do the basic CRUD benchmarks with Go.

## Why another Go YCSB?

+ We want to build a standard benchmark tool in Go.
+ We are not familiar with Java.
+ TiKV only has a Go client.

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
- aerospike

## Database Configuration

You can pass the database configuraitons through `-p field=value` in the command line directly.

### mysql

|field|default value|description|
|-|-|-|
|mysql.host|"127.0.0.1"|MySQL Host|
|mysql.port|3306|MySQL Port|
|mysql.user|"root"|MySQL User|
|mysql.passowrd||MySQL Password|
|mysql.db|"test"|MySQL Database|
|mysql.verbose|false|Output the execution query|
|mysql.droptable|false|Drop Table at first|

### TiKV

|field|default value|description|
|-|-|-|
|tikv.pd|"127.0.0.1:2379"|PD endpoints, seperated by comma|
|tikv.type|"raw"|TiKV mode, "raw", "txn", or "coprocessor"|


### FoundationDB

|field|default value|description|
|-|-|-|
|fdb.cluster|""|The cluster file used for FoundationDB, if not set, will use the [default](https://apple.github.io/foundationdb/administration.html#default-cluster-file)|
|fdb.dbname|"DB"|The cluster database name|
|fdb.apiversion|510|API version, now only 5.1 is supported|

### PostgreSQL

|field|default value|description|
|-|-|-|
|pg.host|"127.0.0.1"|PostgreSQL Host|
|pg.port|5432|PostgreSQL Port|
|pg.user|"root"|PostgreSQL User|
|pg.passowrd||PostgreSQL Password|
|pg.db|"test"|PostgreSQL Database|
|pg.sslmode|"disable|PostgreSQL ssl mode|
|pg.verbose|false|Output the execution query|
|pg.droptable|false|Drop Table at first|

### Aerospike

|field|default value|description|
|-|-|-|
|aerospike.host|"localhost"|The port of the Aerospike service|
|aerospike.port|3000|The port of the Aerospike service|
|aerospike.ns|"test"|The namespace to use|

### Badger

|field|default value|description|
|-|-|-|
|badger.dir|"/tmp/badger"|The directory to save data|
|badger.valuedir|"/tmp/badger"|The directory to save value, if not set, use badger.dir|
|badger.dropdata|false|Whether to remove all data before test|
|badger.sync_writes|true|Sync all writes to disk|
|badger.num_versions_to_keep|1|How many versions to keep per key|
|badger.max_table_size|64MB|Each table (or file) is at most this size|
|badger.level_size_multiplier|10|Equals SizeOf(Li+1)/SizeOf(Li)|
|badger.max_levels|7|Maximum number of levels of compaction|
|badger.value_threshold|32|Maximum number of tables to keep in memory, before stalling|
|badger.num_memtables|5|Maximum number of tables to keep in memory, before stalling|
|badger.num_level0_tables|5|Maximum number of Level 0 tables before we start compacting|
|badger.num_level0_tables_stall|10|If we hit this number of Level 0 tables, we will stall until L0 is compacted away|
|badger.level_one_size|256MB|Maximum total size for L1|
|badger.value_log_file_size|1GB|Size of single value log file|
|badger.value_log_max_entries|1000000|Max number of entries a value log file can hold (approximately). A value log file would be determined by the smaller of its file size and max entries|
|badger.num_compactors|3|Number of compaction workers to run concurrently|
|badger.do_not_compact|false|Stops LSM tree from compactions|
|badger.table_loading_mode|options.LoadToRAM|How should LSM tree be accessed|
|badger.value_log_loading_mode|options.MemoryMap|How should value log be accessed|

### RocksDB

|field|default value|description|
|-|-|-|
|rocksdb.dir|"/tmp/rocksdb"|The directory to save data|
|rocksdb.dropdata|false|Whether to remove all data before test|
|rocksdb.allow_concurrent_memtable_writes|true|Sets whether to allow concurrent memtable writes|
|rocksdb.allow_mmap_reads|false|Enable/Disable mmap reads for reading sst tables|
|rocksdb.allow_mmap_writes|false|Enable/Disable mmap writes for writing sst tables|
|rocksdb.arena_block_size|write_buffer_size / 8|Sets the size of one block in arena memory allocation|
|rocksdb.db_write_buffer_size|0(disable)|Sets the amount of data to build up in memtables across all column families before writing to disk|
|rocksdb.hard_pending_compaction_bytes_limit|256GB|Sets the bytes threshold at which all writes are stopped if estimated bytes needed to be compaction exceed this threshold|
|rocksdb.level0_file_num_compaction_trigger|4|Sets the number of files to trigger level-0 compaction|
|rocksdb.level0_slowdown_writes_trigger|8|Sets the soft limit on number of level-0 files|
|rocksdb.level0_stop_writes_trigger|12|Sets the maximum number of level-0 files. We stop writes at this point|
|rocksdb.max_bytes_for_level_base|10MB|Sets the maximum total data size for a level|
|rocksdb.max_bytes_for_level_multiplier|10|Sets the max Bytes for level multiplier|
|rocksdb.max_total_wal_size|\[sum of all write_buffer_size * max_write_buffer_number\] * 4|Sets the maximum total wal size in bytes. Once write-ahead logs exceed this size, we will start forcing the flush of column families whose memtables are backed by the oldest live WAL file (i.e. the ones that are causing all the space amplification)|
|rocksdb.memtable_huge_page_size|0|Sets the page size for huge page for arena used by the memtable|
|rocksdb.num_levels|7|Sets the number of levels for this database|
|rocksdb.use_direct_reads|false|Enable/Disable direct I/O mode (O_DIRECT) for reads|
|rocksdb.use_fsync|false|Enable/Disable fsync|
|rocksdb.write_buffer_size|4MB|Sets the amount of data to build up in memory (backed by an unsorted log on disk) before converting to a sorted on-disk file|
|rocksdb.block_size|4KB|Sets the approximate size of user data packed per block. Note that the block size specified here corresponds opts uncompressed data. The actual size of the unit read from disk may be smaller if compression is enabled|
|rocksdb.block_size_deviation|10|Sets the block size deviation. This is used opts close a block before it reaches the configured 'block_size'. If the percentage of free space in the current block is less than this specified number and adding a new record opts the block will exceed the configured block size, then this block will be closed and the new record will be written opts the next block|
|rocksdb.cache_index_and_filter_blocks|false|Indicating if we'd put index/filter blocks to the block cache. If not specified, each "table reader" object will pre-load index/filter block during table initialization|
|rocksdb.no_block_cache|false|Specify whether block cache should be used or not|
|rocksdb.pin_l0_filter_and_index_blocks_in_cache|false|Sets cache_index_and_filter_blocks. If is true and the below is true (hash_index_allow_collision), then filter and index blocks are stored in the cache, but a reference is held in the "table reader" object so the blocks are pinned and only evicted from cache when the table reader is freed|
|rocksdb.whole_key_filtering|true|Specify if whole keys in the filter (not just prefixes) should be placed. This must generally be true for gets opts be efficient|
|rocksdb.block_restart_interval|16|Sets the number of keys between restart points for delta encoding of keys. This parameter can be changed dynamically|
|rocksdb.filter_policy|nil|Sets the filter policy opts reduce disk reads. Many applications will benefit from passing the result of NewBloomFilterPolicy() here|
|rocksdb.index_type|kBinarySearch|Sets the index type used for this table. kBinarySearch: A space efficient index block that is optimized for binary-search-based index. kHashSearch: The hash index, if enabled, will do the hash lookup when `Options.prefix_extractor` is provided. kTwoLevelIndexSearch: A two-level index implementation. Both levels are binary search indexes|

## TODO

- [ ] Support more measurement, like HdrHistogram
- [ ] Add tests for generators
