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

package badger

import (
	"context"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

//  properties
const (
	badgerDir      = "badger.dir"
	badgerValueDir = "badger.valuedir"
	badgerDropData = "badger.dropdata"
	badgerSyncWrites = "badger.sync_writes"
	badgerNumVersionsToKeep = "badger.num_versions_to_keep"
	badgerMaxTableSize = "badger.max_table_size"
	badgerLevelSizeMultiplier = "badger.level_size_multiplier"
	badgerMaxLevels = "badger.max_levels"
	badgerValueThreshold = "badger.value_threshold"
	badgerNumMemtables = "badger.num_memtables"
	badgerNumLevelZeroTables = "badger.num_level0_tables"
	badgerNumLevelZeroTablesStall = "badger.num_level0_tables_stall"
	badgerLevelOneSize = "badger.level_one_size"
	badgerValueLogFileSize = "badger.value_log_file_size"
	badgerValueLogMaxEntries = "badger.value_log_max_entries"
	badgerNumCompactors = "badger.num_compactors"
	badgerDoNotCompact = "badger.do_not_compact"
	// TODO: add more configurations
)

type badgerCreator struct {
}

type badgerDB struct {
	p *properties.Properties

	db *badger.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

type contextKey string

const stateKey = contextKey("badgerDB")

type badgerState struct {
}

func (c badgerCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(badgerDropData, false) {
		os.RemoveAll(opts.Dir)
		os.RemoveAll(opts.ValueDir)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &badgerDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func getOptions(p *properties.Properties) badger.Options {
	opts := badger.DefaultOptions
	opts.Dir = p.GetString(badgerDir, "/tmp/badger")
	opts.ValueDir = p.GetString(badgerValueDir, opts.Dir)

	if b := p.GetBool(badgerSyncWrites, false); b {
		opts.SyncWrites = b
	}
	if b := p.GetInt(badgerNumVersionsToKeep, 0); b > 0 {
		opts.NumVersionsToKeep = b
	}
	if b := p.GetInt64(badgerMaxTableSize, 0); b > 0 {
		opts.MaxTableSize = b
	}
	if b := p.GetInt(badgerLevelSizeMultiplier, 0); b > 0 {
		opts.LevelSizeMultiplier = b
	}
	if b := p.GetInt(badgerMaxLevels, 0); b > 0 {
		opts.MaxLevels = b
	}
	if b := p.GetInt(badgerValueThreshold, 0); b > 0 {
		opts.ValueThreshold = b
	}
	if b := p.GetInt(badgerNumMemtables, 0); b > 0 {
		opts.NumMemtables = b
	}
	if b := p.GetInt(badgerNumLevelZeroTables, 0); b > 0 {
		opts.NumLevelZeroTables = b
	}
	if b := p.GetInt(badgerNumLevelZeroTablesStall, 0); b > 0 {
		opts.NumLevelZeroTablesStall = b
	}
	if b := p.GetInt64(badgerLevelOneSize, 0); b > 0 {
		opts.LevelOneSize = b
	}
	if b := p.GetInt64(badgerValueLogFileSize, 0); b > 0 {
		opts.ValueLogFileSize = b
	}
	if b := p.GetUint64(badgerValueLogMaxEntries, 0); b > 0 {
		opts.ValueLogMaxEntries = uint32(b)
	}
	if b := p.GetInt(badgerNumCompactors, 0); b > 0 {
		opts.NumCompactors = b
	}
	if b := p.GetBool(badgerDoNotCompact, false); b {
		opts.DoNotCompact = b
	}

	return opts
}

func (db *badgerDB) Close() error {
	return db.db.Close()
}

func (db *badgerDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *badgerDB) CleanupThread(_ context.Context) {
}

func (db *badgerDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *badgerDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}
		row, err := item.Value()
		if err != nil {
			return err
		}

		m, err = db.r.Decode(row, fields)
		return err
	})

	return m, err
}

func (db *badgerDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	err := db.db.View(func(txn *badger.Txn) error {
		rowStartKey := db.getRowKey(table, startKey)
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		i := 0
		for it.Seek(rowStartKey); it.Valid() && i < count; it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			m, err := db.r.Decode(value, fields)
			if err != nil {
				return err
			}

			res[i] = m
			i++
		}

		return nil
	})

	return res, err
}

func (db *badgerDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)
		item, err := txn.Get(rowKey)
		if err != nil {
			return err
		}

		value, err := item.Value()
		if err != nil {
			return err
		}

		data, err := db.r.Decode(value, nil)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer db.bufPool.Put(buf)

		rowData, err := db.r.Encode(buf.Bytes(), data)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, rowData)
	})
	return err
}

func (db *badgerDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		rowKey := db.getRowKey(table, key)

		buf := db.bufPool.Get()
		defer db.bufPool.Put(buf)

		rowData, err := db.r.Encode(buf.Bytes(), values)
		if err != nil {
			return err
		}
		return txn.Set(rowKey, rowData)
	})

	return err
}

func (db *badgerDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(db.getRowKey(table, key))
	})

	return err
}

func init() {
	ycsb.RegisterDBCreator("badger", badgerCreator{})
}
