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
	rocksdbAllowConcurrentMemtableWrites = "rocksdb.allow_concurrent_memtable_writes"
	rocsdbAllowMmapReads = "rocksdb.allow_mmap_reads"
	rocksdbAllowMmapWrites = "rocksdb.allow_mmap_writes"
	rocksdbSetArenaBlockSize = "rocksdb.set_arena_block_size"
	rocksdbSetDBWriteBufferSize = "rocksdb.set_db_write_buffer_size"
	rocksdbSetHardPendingCompactionBytesLimit = "rocksdb.set_hard_pending_compaction_bytes_limit"
	rocksdbSetHardRateLimit = "rocksdb.set_hard_rate_limit"
	rocksdbSetLevel0FileNumCompactionTrigger = "rocksdb.set_level0_file_num_compaction_trigger"
	rocksdbSetLevel0SlowdownWritesTrigger = "rocksdb.set_level0_slow_down_writes_trigger"
	rocksdbSetLevel0StopWritesTrigger = "rocksdb.set_level0_stop_writes_trigger"
	rocksdbSetMaxBackgroundCompactions = "rocksdb.set_max_background_compactions"
	rocksdbSetMaxBackgroundFlushes = "rocksdb.set_max_background_flushes"
	rocksdbSetMaxBytesForLevelBase = "rocksdb.set_max_bytes_for_level_base"
	rocksdbSetMaxBytesForLevelMultiplier = "rocksdb.set_max_bytes_for_level_multiplier"
	rocksdbSetMaxTotalWalSize = "rocksdb.set_max_total_wal_size"
	rocksdbSetMemtableHugePageSize = "rocksdb.set_memtable_huge_page_size"
	rocksdbSetNumLevels = "rocksdb.set_num_levels"
	rocksdbSetUseDirectReads = "rocksdb.set_use_direct_reads"
	rocksdbSetUseFsync = "rocksdb.set_use_fsync"
	rocksdbSetWriteBufferSize = "rocksdb.set_write_buffer_size"
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
	if b := p.GetInt(rocksdbSetArenaBlockSize, 0); b > 0 {
		opts.SetArenaBlockSize(b)
	}
	if b := p.GetInt(rocksdbSetDBWriteBufferSize, 0); b > 0 {
		opts.SetDbWriteBufferSize(b)
	}
	if b := p.GetUint64(rocksdbSetHardPendingCompactionBytesLimit, 0); b > 0 {
		opts.SetHardPendingCompactionBytesLimit(b)
	}
	if b := p.GetFloat64(rocksdbSetHardRateLimit, 0); b > 0 {
		opts.SetHardRateLimit(b)
	}
	if b := p.GetInt(rocksdbSetLevel0FileNumCompactionTrigger, 0); b > 0 {
		opts.SetLevel0FileNumCompactionTrigger(b)
	}
	if b := p.GetInt(rocksdbSetLevel0SlowdownWritesTrigger, 0); b > 0 {
		opts.SetLevel0SlowdownWritesTrigger(b)
	}
	if b := p.GetInt(rocksdbSetLevel0StopWritesTrigger, 0); b > 0 {
		opts.SetLevel0StopWritesTrigger(b)
	}
	if b := p.GetInt(rocksdbSetMaxBackgroundCompactions, 0); b > 0 {
		opts.SetMaxBackgroundCompactions(b)
	}
	if b := p.GetInt(rocksdbSetMaxBackgroundFlushes, 0); b > 0 {
		opts.SetMaxBackgroundFlushes(b)
	}
	if b := p.GetUint64(rocksdbSetMaxBytesForLevelBase, 0); b > 0 {
		opts.SetMaxBytesForLevelBase(b)
	}
	if b := p.GetFloat64(rocksdbSetMaxBytesForLevelMultiplier, 0); b > 0 {
		opts.SetMaxBytesForLevelMultiplier(b)
	}
	if b := p.GetUint64(rocksdbSetMaxTotalWalSize, 0); b > 0 {
		opts.SetMaxTotalWalSize(b)
	}
	if b := p.GetInt(rocksdbSetMemtableHugePageSize, 0); b > 0 {
		opts.SetMemtableHugePageSize(b)
	}
	if b := p.GetInt(rocksdbSetNumLevels, 0); b > 0 {
		opts.SetNumLevels(b)
	}
	if b := p.GetBool(rocksdbSetUseDirectReads, false); b {
		opts.SetUseDirectReads(b)
	}
	if b := p.GetBool(rocksdbSetUseFsync, false); b {
		opts.SetUseFsync(b)
	}
	if b := p.GetInt(rocksdbSetWriteBufferSize, 0); b > 0 {
		opts.SetWriteBufferSize(b)
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
