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

// +build rocksdb

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

	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)

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
