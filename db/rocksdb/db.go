// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rocksdb

import (
	"context"
	"fmt"
	"os"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tecbot/gorocksdb"
)

//  properties
const (
	rocksdbDir      = "rocksdb.dir"
	rocksdbDropData = "rocksdb.dropdata"
	// DBOptions
	rocksdbAllowConcurrentMemtableWrites   = "rocksdb.allow_concurrent_memtable_writes"
	rocsdbAllowMmapReads                   = "rocksdb.allow_mmap_reads"
	rocksdbAllowMmapWrites                 = "rocksdb.allow_mmap_writes"
	rocksdbArenaBlockSize                  = "rocksdb.arena_block_size"
	rocksdbDBWriteBufferSize               = "rocksdb.db_write_buffer_size"
	rocksdbHardPendingCompactionBytesLimit = "rocksdb.hard_pending_compaction_bytes_limit"
	rocksdbLevel0FileNumCompactionTrigger  = "rocksdb.level0_file_num_compaction_trigger"
	rocksdbLevel0SlowdownWritesTrigger     = "rocksdb.level0_slowdown_writes_trigger"
	rocksdbLevel0StopWritesTrigger         = "rocksdb.level0_stop_writes_trigger"
	rocksdbMaxBackgroundFlushes            = "rocksdb.max_background_flushes"
	rocksdbMaxBytesForLevelBase            = "rocksdb.max_bytes_for_level_base"
	rocksdbMaxBytesForLevelMultiplier      = "rocksdb.max_bytes_for_level_multiplier"
	rocksdbMaxTotalWalSize                 = "rocksdb.max_total_wal_size"
	rocksdbMemtableHugePageSize            = "rocksdb.memtable_huge_page_size"
	rocksdbNumLevels                       = "rocksdb.num_levels"
	rocksdbUseDirectReads                  = "rocksdb.use_direct_reads"
	rocksdbUseFsync                        = "rocksdb.use_fsync"
	rocksdbWriteBufferSize                 = "rocksdb.write_buffer_size"
	// TableOptions/BlockBasedTable
	rocksdbBlockSize                        = "rocksdb.block_size"
	rocksdbBlockSizeDeviation               = "rocksdb.block_size_deviation"
	rocksdbCacheIndexAndFilterBlocks        = "rocksdb.cache_index_and_filter_blocks"
	rocksdbNoBlockCache                     = "rocksdb.no_block_cache"
	rocksdbPinL0FilterAndIndexBlocksInCache = "rocksdb.pin_l0_filter_and_index_blocks_in_cache"
	rocksdbWholeKeyFiltering                = "rocksdb.whole_key_filtering"
	rocksdbBlockRestartInterval             = "rocksdb.block_restart_interval"
	rocksdbFilterPolicy                     = "rocksdb.filter_policy"
	rocksdbIndexType                        = "rocksdb.index_type"
	// TODO: add more configurations
)

type rocksDBCreator struct {
}

type rocksDB struct {
	p *properties.Properties

	db *gorocksdb.DB

	r       *util.RowCodec
	bufPool *util.BufPool

	readOpts  *gorocksdb.ReadOptions
	writeOpts *gorocksdb.WriteOptions
}

type contextKey string

func (c rocksDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dir := p.GetString(rocksdbDir, "/tmp/rocksdb")

	if p.GetBool(rocksdbDropData, false) {
		os.RemoveAll(dir)
	}

	opts := getOptions(p)

	db, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return nil, err
	}

	return &rocksDB{
		p:         p,
		db:        db,
		r:         util.NewRowCodec(p),
		bufPool:   util.NewBufPool(),
		readOpts:  gorocksdb.NewDefaultReadOptions(),
		writeOpts: gorocksdb.NewDefaultWriteOptions(),
	}, nil
}

func getTableOptions(p *properties.Properties) (*gorocksdb.BlockBasedTableOptions, bool) {
	hasTableOption := false
	tblOpts := gorocksdb.NewDefaultBlockBasedTableOptions()

	if b := p.GetInt(rocksdbBlockSize, 0); b > 0 {
		tblOpts.SetBlockSize(b)
		hasTableOption = true
	}
	if b := p.GetInt(rocksdbBlockSizeDeviation, 0); b > 0 {
		tblOpts.SetBlockSizeDeviation(b)
		hasTableOption = true
	}
	if b := p.GetBool(rocksdbCacheIndexAndFilterBlocks, false); b {
		tblOpts.SetCacheIndexAndFilterBlocks(b)
		hasTableOption = true
	}
	if b := p.GetBool(rocksdbNoBlockCache, false); b {
		tblOpts.SetNoBlockCache(b)
		hasTableOption = true
	}
	if b := p.GetBool(rocksdbPinL0FilterAndIndexBlocksInCache, false); b {
		tblOpts.SetPinL0FilterAndIndexBlocksInCache(b)
		hasTableOption = true
	}
	if b := p.GetBool(rocksdbWholeKeyFiltering, false); b {
		tblOpts.SetWholeKeyFiltering(b)
		hasTableOption = true
	}
	if b := p.GetInt(rocksdbBlockRestartInterval, 0); b > 0 {
		tblOpts.SetBlockRestartInterval(b)
		hasTableOption = true
	}
	if b := p.GetString(rocksdbFilterPolicy, ""); len(b) > 0 {
		if b == "rocksdb.BuiltinBloomFilter" {
			const defaultBitsPerKey = 10
			tblOpts.SetFilterPolicy(gorocksdb.NewBloomFilter(defaultBitsPerKey))
			hasTableOption = true
		}
	}
	if b := p.GetString(rocksdbIndexType, ""); len(b) > 0 {
		if b == "KBinarySearch" {
			tblOpts.SetIndexType(gorocksdb.KBinarySearchIndexType)
			hasTableOption = true
		} else if b == "KHashSearch" {
			tblOpts.SetIndexType(gorocksdb.KHashSearchIndexType)
			hasTableOption = true
		} else if b == "KTwoLevelIndexSearch" {
			tblOpts.SetIndexType(gorocksdb.KTwoLevelIndexSearchIndexType)
			hasTableOption = true
		}
	}

	return tblOpts, hasTableOption
}

func getOptions(p *properties.Properties) *gorocksdb.Options {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

	if b := p.GetBool(rocksdbAllowConcurrentMemtableWrites, false); b {
		opts.SetAllowConcurrentMemtableWrites(b)
	}
	if b := p.GetBool(rocsdbAllowMmapReads, false); b {
		opts.SetAllowMmapReads(b)
	}
	if b := p.GetBool(rocksdbAllowMmapWrites, false); b {
		opts.SetAllowMmapWrites(b)
	}
	if b := p.GetInt(rocksdbArenaBlockSize, 0); b > 0 {
		opts.SetArenaBlockSize(b)
	}
	if b := p.GetInt(rocksdbDBWriteBufferSize, 0); b > 0 {
		opts.SetDbWriteBufferSize(b)
	}
	if b := p.GetUint64(rocksdbHardPendingCompactionBytesLimit, 0); b > 0 {
		opts.SetHardPendingCompactionBytesLimit(b)
	}
	if b := p.GetInt(rocksdbLevel0FileNumCompactionTrigger, 0); b > 0 {
		opts.SetLevel0FileNumCompactionTrigger(b)
	}
	if b := p.GetInt(rocksdbLevel0SlowdownWritesTrigger, 0); b > 0 {
		opts.SetLevel0SlowdownWritesTrigger(b)
	}
	if b := p.GetInt(rocksdbLevel0StopWritesTrigger, 0); b > 0 {
		opts.SetLevel0StopWritesTrigger(b)
	}
	if b := p.GetInt(rocksdbMaxBackgroundFlushes, 0); b > 0 {
		opts.SetMaxBackgroundFlushes(b)
	}
	if b := p.GetUint64(rocksdbMaxBytesForLevelBase, 0); b > 0 {
		opts.SetMaxBytesForLevelBase(b)
	}
	if b := p.GetFloat64(rocksdbMaxBytesForLevelMultiplier, 0); b > 0 {
		opts.SetMaxBytesForLevelMultiplier(b)
	}
	if b := p.GetUint64(rocksdbMaxTotalWalSize, 0); b > 0 {
		opts.SetMaxTotalWalSize(b)
	}
	if b := p.GetInt(rocksdbMemtableHugePageSize, 0); b > 0 {
		opts.SetMemtableHugePageSize(b)
	}
	if b := p.GetInt(rocksdbNumLevels, 0); b > 0 {
		opts.SetNumLevels(b)
	}
	if b := p.GetBool(rocksdbUseDirectReads, false); b {
		opts.SetUseDirectReads(b)
	}
	if b := p.GetBool(rocksdbUseFsync, false); b {
		opts.SetUseFsync(b)
	}
	if b := p.GetInt(rocksdbWriteBufferSize, 0); b > 0 {
		opts.SetWriteBufferSize(b)
	}

	if tblOpts, ok := getTableOptions(p); ok {
		opts.SetBlockBasedTableFactory(tblOpts)
	}

	return opts
}

func (db *rocksDB) Close() error {
	db.db.Close()
	return nil
}

func (db *rocksDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rocksDB) CleanupThread(_ context.Context) {
}

func (db *rocksDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func cloneValue(v *gorocksdb.Slice) []byte {
	return append([]byte(nil), v.Data()...)
}

func (db *rocksDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.readOpts, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	}
	defer value.Free()

	return db.r.Decode(cloneValue(value), fields)
}

func (db *rocksDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	it := db.db.NewIterator(db.readOpts)
	defer it.Close()

	rowStartKey := db.getRowKey(table, startKey)

	it.Seek(rowStartKey)
	i := 0
	for it = it; it.Valid() && i < count; it.Next() {
		value := it.Value()
		m, err := db.r.Decode(cloneValue(value), fields)
		if err != nil {
			return nil, err
		}
		res[i] = m
		i++
	}

	if err := it.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *rocksDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)

	return db.db.Put(db.writeOpts, rowKey, rowData)
}

func (db *rocksDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}
	return db.db.Put(db.writeOpts, rowKey, rowData)
}

func (db *rocksDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)

	return db.db.Delete(db.writeOpts, rowKey)
}

func init() {
	ycsb.RegisterDBCreator("rocksdb", rocksDBCreator{})
}
