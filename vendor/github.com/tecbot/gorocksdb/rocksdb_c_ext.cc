#include "rocksdb_c_ext.h"

#include <stdlib.h>
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/perf_context.h"

using rocksdb::BytewiseComparator;
using rocksdb::Cache;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::CompactionFilter;
using rocksdb::CompactionFilterFactory;
using rocksdb::CompactionFilterContext;
using rocksdb::CompactionOptionsFIFO;
using rocksdb::Comparator;
using rocksdb::CompressionType;
using rocksdb::WALRecoveryMode;
using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::DbPath;
using rocksdb::Env;
using rocksdb::EnvOptions;
using rocksdb::InfoLogLevel;
using rocksdb::FileLock;
using rocksdb::FilterPolicy;
using rocksdb::FlushOptions;
using rocksdb::IngestExternalFileOptions;
using rocksdb::Iterator;
using rocksdb::Logger;
using rocksdb::NewBloomFilterPolicy;
using rocksdb::NewLRUCache;
using rocksdb::Options;
using rocksdb::BlockBasedTableOptions;
using rocksdb::CuckooTableOptions;
using rocksdb::RandomAccessFile;
using rocksdb::Range;
using rocksdb::ReadOptions;
using rocksdb::SequentialFile;
using rocksdb::Slice;
using rocksdb::SliceParts;
using rocksdb::SliceTransform;
using rocksdb::Snapshot;
using rocksdb::SstFileWriter;
using rocksdb::Status;
using rocksdb::WritableFile;
using rocksdb::WriteBatch;
using rocksdb::WriteBatchWithIndex;
using rocksdb::WriteOptions;
using rocksdb::LiveFileMetaData;
using rocksdb::BackupEngine;
using rocksdb::BackupableDBOptions;
using rocksdb::BackupInfo;
using rocksdb::BackupID;
using rocksdb::RestoreOptions;
using rocksdb::CompactRangeOptions;
using rocksdb::BottommostLevelCompaction;
using rocksdb::RateLimiter;
using rocksdb::NewGenericRateLimiter;
using rocksdb::PinnableSlice;
using rocksdb::TransactionDBOptions;
using rocksdb::TransactionDB;
using rocksdb::TransactionOptions;
using rocksdb::OptimisticTransactionDB;
using rocksdb::OptimisticTransactionOptions;
using rocksdb::Transaction;
using rocksdb::Checkpoint;
using rocksdb::TransactionLogIterator;
using rocksdb::BatchResult;
using rocksdb::PerfLevel;
using rocksdb::PerfContext;

using std::shared_ptr;

extern "C" {

struct rocksdb_options_t         { Options           rep; };
struct rocksdb_block_based_table_options_t  { BlockBasedTableOptions rep; };

/* Options */

void rocksdb_options_set_max_background_jobs(rocksdb_options_t* opt, int n) {
  opt->rep.max_background_jobs = n;
}

/* Block based table options */

void rocksdb_block_based_options_set_block_align(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.block_align = v;
}

}  // end extern "C"