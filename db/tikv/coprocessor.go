//  Copyright 2018 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package tikv

import (
	"context"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pkg/errors"
)

type coprocessor struct {
	db      kv.Storage
	table   *util.Table
	bufPool *util.BufPool
}

func (db *coprocessor) Close() error {
	return db.db.Close()
}

func (db *coprocessor) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (db *coprocessor) CleanupThread(ctx context.Context) {
}

func (db *coprocessor) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	return nil, errors.New("unsupported type of read in coprocessor")
}

func (db *coprocessor) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	panic("implement me")
}

func (db *coprocessor) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return errors.New("unsupported type of update in coprocessor")
}

func (db *coprocessor) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)
	// encode key
	rowKey := db.table.EncodeKey(key)
	// encode value
	rowValue, err := db.table.EncodeValue(buf.Bytes(), values)
	if err != nil {
		return err
	}
	// do transaction
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = tx.Set(rowKey, rowValue); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *coprocessor) Delete(ctx context.Context, table string, key string) error {
	return errors.New("unsupported type of delete in coprocessor")
}

func createCoprocessorDB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	tikv.MaxConnectionCount = 128
	driver := tikv.Driver{}
	db, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddr))
	if err != nil {
		return nil, err
	}
	return &coprocessor{db: db, table: util.NewTable(p), bufPool: util.NewBufPool()}, nil
}
