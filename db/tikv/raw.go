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
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
)

type rawDB struct {
	db      *tikv.RawKVClient
	r       *util.RowCodec
	bufPool *util.BufPool
}

func createRawDB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	tikv.MaxConnectionCount = 128
	db, err := tikv.NewRawKVClient(strings.Split(pdAddr, ","), config.Security{})
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &rawDB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool}, nil
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

func (db *rawDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	row, err := db.db.Get(db.getRowKey(table, key))
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row, fields)
}

func (db *rawDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	_, rows, err := db.db.Scan(db.getRowKey(table, startKey), count)
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

func (db *rawDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.Delete(db.getRowKey(table, key))
}
