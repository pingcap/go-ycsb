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

package tikv

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

type rawDB struct {
	db      *rawkv.Client
	r       *util.RowCodec
	bufPool *util.BufPool
}

func createRawDB(p *properties.Properties, conf config.Config) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	db, err := rawkv.NewClient(strings.Split(pdAddr, ","), conf)
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &rawDB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
	}, nil
}

func (db *rawDB) Close() error {
	return db.db.Close()
}

func (db *rawDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rawDB) CleanupThread(ctx context.Context) {
}

func (db *rawDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *rawDB) ToSqlDB() *sql.DB {
	return nil
}

func (db *rawDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	row, err := db.db.Get(db.getRowKey(table, key))
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row, fields)
}

func (db *rawDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	values, err := db.db.BatchGet(rowKeys)
	if err != nil {
		return nil, err
	}
	rowValues := make([]map[string][]byte, len(keys))

	for i, value := range values {
		if len(value) > 0 {
			rowValues[i], err = db.r.Decode(value, fields)
		} else {
			rowValues[i] = nil
		}
	}
	return rowValues, nil
}

func (db *rawDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	_, rows, err := db.db.Scan(db.getRowKey(table, startKey), nil, count)
	if err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

func (db *rawDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	row, err := db.db.Get(db.getRowKey(table, key))
	if err != nil {
		return nil
	}

	data, err := db.r.Decode(row, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	return db.Insert(ctx, table, key, data)
}

func (db *rawDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		// TODO should we check the key exist?
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.BatchPut(rawKeys, rawValues)
}

func (db *rawDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}

	return db.db.Put(db.getRowKey(table, key), rowData)
}

func (db *rawDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.BatchPut(rawKeys, rawValues)
}

func (db *rawDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(db.getRowKey(table, key))
}

func (db *rawDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	return db.db.BatchDelete(rowKeys)
}
