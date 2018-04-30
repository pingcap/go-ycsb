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

// +build foundationdb

package foundationdb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

const (
	fdbClusterFile = "fdb.cluster"
	fdbDatabase    = "fdb.dbname"
	fdbAPIVersion  = "fdb.apiversion"
)

type fDB struct {
	db           fdb.Database
	fieldIndices map[string]int64
	fields       []string
	bufPool      *util.BufPool
}

func createDB(p *properties.Properties) (ycsb.DB, error) {
	clusterFile := p.GetString(fdbClusterFile, "")
	database := p.GetString(fdbDatabase, "DB")
	apiVersion := p.GetInt(fdbAPIVersion, 510)

	fdb.MustAPIVersion(apiVersion)

	db, err := fdb.Open(clusterFile, []byte(database))
	if err != nil {
		return nil, err
	}

	fieldIndices := util.CreateFieldIndices(p)
	fields := util.AllFields(p)
	bufPool := util.NewBufPool()

	return &fDB{
		db:           db,
		fieldIndices: fieldIndices,
		fields:       fields,
		bufPool:      bufPool,
	}, nil
}

func (db *fDB) Close() error {
	return nil
}

func (db *fDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *fDB) CleanupThread(ctx context.Context) {
}

func (db *fDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *fDB) getEndRowKey(table string) []byte {
	// ';' is ':' + 1 in the ASCII
	return util.Slice(fmt.Sprintf("%s;", table))
}

func (db *fDB) decodeRow(ctx context.Context, row []byte, fields []string) (map[string][]byte, error) {
	if len(fields) == 0 {
		fields = db.fields
	}

	cols := make(map[int64]*types.FieldType, len(fields))
	fieldType := types.NewFieldType(mysql.TypeVarchar)

	for _, field := range fields {
		i := db.fieldIndices[field]
		cols[i] = fieldType
	}

	data, err := tablecodec.DecodeRow(row, cols, nil)
	if err != nil {
		return nil, err
	}

	res := make(map[string][]byte, len(fields))
	for _, field := range fields {
		i := db.fieldIndices[field]
		if v, ok := data[i]; ok {
			res[field] = v.GetBytes()
		}
	}

	return res, nil
}

func (db *fDB) createRowData(buf *bytes.Buffer, values map[string][]byte) ([]byte, error) {
	cols := make([]types.Datum, 0, len(values))
	colIDs := make([]int64, 0, len(values))

	for k, v := range values {
		i := db.fieldIndices[k]
		var d types.Datum
		d.SetBytes(v)

		cols = append(cols, d)
		colIDs = append(colIDs, i)
	}

	rowData, err := tablecodec.EncodeRow(&stmtctx.StatementContext{}, cols, colIDs, buf.Bytes(), nil)
	return rowData, err
}

func (db *fDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	rowKey := db.getRowKey(table, key)
	row, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		f := tr.Get(fdb.Key(rowKey))
		return f.Get()
	})

	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.decodeRow(ctx, row.([]byte), fields)
}

func (db *fDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	rowKey := db.getRowKey(table, startKey)
	res, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		r := fdb.KeyRange{
			Begin: fdb.Key(rowKey),
			End:   fdb.Key(db.getEndRowKey(table)),
		}
		ri := tr.GetRange(r, fdb.RangeOptions{Limit: count}).Iterator()
		res := make([]map[string][]byte, 0, count)
		for ri.Advance() {
			kv, err := ri.Get()
			if err != nil {
				return nil, err
			}

			if kv.Value == nil {
				res = append(res, nil)
			} else {
				v, err := db.decodeRow(ctx, kv.Value, fields)
				if err != nil {
					return nil, err
				}
				res = append(res, v)
			}

		}

		return res, nil
	})
	if err != nil {
		return nil, err
	}
	return res.([]map[string][]byte), nil
}

func (db *fDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		f := tr.Get(fdb.Key(rowKey))
		row, err := f.Get()
		if err != nil {
			return nil, err
		} else if row == nil {
			return nil, nil
		}

		data, err := db.decodeRow(ctx, row, nil)
		if err != nil {
			return nil, err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer db.bufPool.Put(buf)

		rowData, err := db.createRowData(buf, data)
		if err != nil {
			return nil, err
		}

		tr.Set(fdb.Key(rowKey), rowData)
		return
	})

	return err
}

func (db *fDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.createRowData(buf, values)
	if err != nil {
		return err
	}

	rowKey := db.getRowKey(table, key)
	_, err = db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Set(fdb.Key(rowKey), rowData)
		return
	})
	return err
}

func (db *fDB) Delete(ctx context.Context, table string, key string) error {
	rowKey := db.getRowKey(table, key)
	_, err := db.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.Clear(fdb.Key(rowKey))
		return
	})
	return err
}

type fdbCreator struct {
}

func (c fdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return createDB(p)
}

func init() {
	ycsb.RegisterDBCreator("fdb", fdbCreator{})
	ycsb.RegisterDBCreator("foundationdb", fdbCreator{})
}
