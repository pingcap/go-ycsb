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
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
)

type txnDB struct {
	db      *txnkv.Client
	r       *util.RowCodec
	bufPool *util.BufPool
}

func createTxnDB(p *properties.Properties, conf config.Config) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	db, err := txnkv.NewClient(strings.Split(pdAddr, ","), conf)
	if err != nil {
		return nil, err
	}

	bufPool := util.NewBufPool()

	return &txnDB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: bufPool}, nil
}

func (db *txnDB) Close() error {
	return db.db.Close()
}

func (db *txnDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *txnDB) CleanupThread(ctx context.Context) {
}

func (db *txnDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *txnDB) ToSqlDB() *sql.DB {
	return nil
}

func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row, err := tx.Get(db.getRowKey(table, key))
	if kv.IsErrNotFound(err) {
		return nil, nil
	} else if row == nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return db.r.Decode(row, fields)
}

func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rowValues := make([]map[string][]byte, len(keys))
	for i, key := range keys {
		value, err := tx.Get(db.getRowKey(table, key))
		if kv.IsErrNotFound(err) || value == nil {
			rowValues[i] = nil
		} else {
			rowValues[i], err = db.r.Decode(value, fields)
			if err != nil {
				return nil, err
			}
		}
	}

	return rowValues, nil
}

func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	it, err := tx.Iter(db.getRowKey(table, startKey), nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	rows := make([][]byte, 0, count)
	for i := 0; i < count && it.Valid(); i++ {
		value := append([]byte{}, it.Value()...)
		rows = append(rows, value)
		if err = it.Next(); err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
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

func (db *txnDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)

	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row, err := tx.Get(rowKey)
	if kv.IsErrNotFound(err) {
		return nil
	} else if row == nil {
		return err
	}

	data, err := db.r.Decode(row, nil)
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

	if err := tx.Set(rowKey, rowData); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, key := range keys {
		// TODO should we check the key exist?
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (db *txnDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}

	tx, err := db.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, key := range keys {
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (db *txnDB) Delete(ctx context.Context, table string, key string) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	err = tx.Delete(db.getRowKey(table, key))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, key := range keys {
		if err != nil {
			return err
		}
		err = tx.Delete(db.getRowKey(table, key))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
