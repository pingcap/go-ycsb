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

package util

import (
	"github.com/pingcap/tipb/go-tipb"
	"github.com/magiconair/properties"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/kv"
)

type Table struct {
	columns []tipb.ColumnInfo
	rowC    *RowCodec
	tableId int64
}

func NewTable(p *properties.Properties) *Table {
	table := &Table{tableId: 1001}
	table.rowC = NewRowCodec(p)
	colLen := len(table.rowC.fields)
	table.columns = make([]tipb.ColumnInfo, 0, colLen)

	return table
}


func (t *Table) EncodeKey(key string) kv.Key {
	return tablecodec.EncodeRowKey(t.tableId, []byte(key))
}

func (t *Table) DecodeValue(row []byte, field []string) (map[string][]byte, error) {

	return nil, nil
}
func (t *Table) EncodeValue(buf []byte, values map[string][]byte) ([]byte, error) {

	return nil, nil
}
