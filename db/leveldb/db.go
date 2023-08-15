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

package leveldb

import (
	"context"
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	leveldbEndpoint = "leveldb.endpoint"
)

type levelDB struct {
	client  LeveldbClient
	r       *util.RowCodec
	bufPool *util.BufPool
}
type leveldbCreator struct {
}

func (c leveldbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	fmt.Println("=====================  Taas - LevelDB  ============================")
	db := new(levelDB)
	err := db.client.Connect(p)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	db.r = util.NewRowCodec(p)
	db.bufPool = util.NewBufPool()
	return db, nil
}

func (db *levelDB) CommitToTaas(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	//TODO implement me
	panic("implement me")
}

func (db *levelDB) Close() error {

	return nil
}

func (db *levelDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *levelDB) CleanupThread(ctx context.Context) {
}

func (db *levelDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *levelDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	// fmt.Println("do Read")
	rowKey := db.getRowKey(table, key)
	row_value, err := db.client.Get(rowKey)
	if err != nil {
		return nil, err
	}
	return db.r.Decode(row_value, fields)
}

func (db *levelDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	fmt.Println("do batchread")
	row_value := make([]map[string][]byte, len(keys))
	for i, key := range keys {
		value, err := db.client.Get(db.getRowKey(table, key))
		if value == nil || err != nil {
			row_value[i] = nil
		} else {
			row_value[i], err = db.r.Decode(value, fields)
			if err != nil {
				return nil, err
			}
		}
	}

	return row_value, nil
}

func (db *levelDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	fmt.Println("do scan")
	rows := make([]map[string][]byte, 0, count)

	// it, err := tx.Iter(db.getRowKey(table, startKey), nil)
	// 如何获取到key迭代器？
	key := startKey

	for i := 0; i < count; i++ {
		row_key := db.getRowKey(table, key)
		row_value, err := db.client.Get(row_key)
		if err != nil {
			return nil, err
		}
		v, err := db.r.Decode(row_value, fields)
		if err != nil {
			return nil, err
		}
		rows[i] = v
	}

	return rows, nil
}

func (db *levelDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	m, err := db.Read(ctx, table, key, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		m[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err = db.r.Encode(buf, m)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	return db.client.Put(rowKey, buf)
}

func (db *levelDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	fmt.Println("do batchupdate")
	// 批量更新？
	for i, key := range keys {
		row_key := db.getRowKey(table, key)
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		if err = db.client.Put(row_key, rowData); err != nil {
			return err
		}
	}
	return nil
}

func (db *levelDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {

	fmt.Println("do insert")
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	rowKey := db.getRowKey(table, key)
	db.client.Put(rowKey, buf)
	return err
}

func (db *levelDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	fmt.Println("do batchinsert")
	// fmt.Println(keys)
	for i, key := range keys {
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rowKey := db.getRowKey(table, key)

		if err = db.client.Put(rowKey, rowData); err != nil {
			return err
		}
	}
	return nil
}

func (db *levelDB) Delete(ctx context.Context, table string, key string) error {
	fmt.Println("do delete")
	return nil
}

func (db *levelDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	fmt.Println("do batchdelete")
	return nil
}

func init() {
	ycsb.RegisterDBCreator("leveldb", leveldbCreator{})
}
