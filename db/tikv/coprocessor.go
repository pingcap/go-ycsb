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
)

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
	// construct the req
	req := kv.Request{Concurrency: 1, Tp: kv.ReqTypeDAG}
	req.KeyRanges = []kv.KeyRange{db.table.GetPointRange(key)}
	dag := db.table.BuildDAGTableScanReq(fields)
	req.StartTs = dag.StartTs
	data, err := dag.Marshal()
	if err != nil {
		return nil, err
	}
	req.Data = data
	// send request
	client := db.db.GetClient()
	res := client.Send(ctx, &req, nil)
	defer res.Close()
	_, err = res.Next(ctx)
	if err != nil {
		return nil, err
	}

	//return db.table.DecodeValue(result.GetData(), fields)
	return make(map[string][]byte, len(fields)), nil
}

func (db *coprocessor) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	// construct the req
	req := kv.Request{Concurrency: 1, Tp: kv.ReqTypeDAG}
	req.KeyRanges = []kv.KeyRange{db.table.GetScanRange(startKey)}
	dag := db.table.BuildDAGTableScanWithLimitReq(fields, count)
	req.StartTs = dag.StartTs
	data, err := dag.Marshal()
	if err != nil {
		return nil, err
	}
	req.Data = data
	// send req
	client := db.db.GetClient()
	res := client.Send(ctx, &req, nil)
	defer res.Close()
	for {
		resSubset, err := res.Next(ctx)
		if err != nil {
			return nil, err
		} else if resSubset == nil {
			break
		}
	}

	return make([]map[string][]byte, count), nil
}

func (db *coprocessor) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	// encode key
	rowKey := db.table.EncodeKey(key)
	// do update transaction
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}

	defer tx.Rollback()
	rowValue, err := tx.Get(rowKey)
	if kv.ErrNotExist.Equal(err) {
		return nil
	} else if rowValue == nil {
		return err
	}
	rowData, err := db.table.DecodeValue(rowValue, nil)
	if err != nil {
		return err
	}
	for field, value := range values {
		rowData[field] = value
	}

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowValue, err = db.table.EncodeValue(buf.Bytes(), rowData)
	if err != nil {
		return err
	}
	if err = tx.Set(rowKey, rowValue); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *coprocessor) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)
	// encode key
	rowKey := db.table.EncodeKey(key)
	// encode values
	rowValue, err := db.table.EncodeValue(buf.Bytes(), values)
	if err != nil {
		return err
	}
	// do insert transaction
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
	// encode key
	rowKey := db.table.EncodeKey(key)
	// do delete transaction
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rowValue, err := tx.Get(rowKey)
	if kv.ErrNotExist.Equal(err) {
		return nil
	} else if rowValue == nil {
		return err
	}

	err = tx.Delete(rowKey)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
