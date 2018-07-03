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
	"math"

	"github.com/magiconair/properties"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
)

// Table is a helper struct to build DAG request,
// as well as encode and decode key and value.
type Table struct {
	columns []tipb.ColumnInfo
	rowC    *RowCodec
	tableID int64
}

// NewTable creates a Table.
func NewTable(p *properties.Properties) *Table {
	table := &Table{tableID: 1001}
	table.rowC = NewRowCodec(p)
	colLen := len(table.rowC.fields)
	table.columns = make([]tipb.ColumnInfo, 0, colLen)
	for i := 0; i < colLen; i++ {
		table.columns = append(table.columns, tipb.ColumnInfo{
			PkHandle:  false,
			ColumnId:  int64(i),
			Collation: int32(mysql.DefaultCollationID),
			ColumnLen: types.UnspecifiedLength,
			Decimal:   types.UnspecifiedLength,
			Tp:        int32(mysql.TypeVarchar)})
	}
	return table
}

// BuildDAGTableScanReq returns a DAGRequest which just contains the table scan executor.
func (t *Table) BuildDAGTableScanReq(fields []string) *tipb.DAGRequest {
	dag := t.buildDAGReq(fields)
	dag.Executors = []*tipb.Executor{t.getTableScanExec(dag.OutputOffsets)}
	return dag
}

// BuildDAGTableScanWithLimitReq returns a DAGRequest which contain table scan and limit executor.
func (t *Table) BuildDAGTableScanWithLimitReq(fields []string, count int) *tipb.DAGRequest {
	dag := t.buildDAGReq(fields)
	// add scan executor
	dag.Executors = []*tipb.Executor{t.getTableScanExec(dag.OutputOffsets)}
	// add limit executor
	dag.Executors = append(dag.Executors, t.getLimitExec(count))
	return dag
}

func (t *Table) buildDAGReq(fields []string) *tipb.DAGRequest {
	dag := &tipb.DAGRequest{}
	dag.StartTs = math.MaxInt64
	var output []uint32
	if len(fields) == 0 {
		output = make([]uint32, 0, len(t.columns))
	} else {
		output = make([]uint32, 0, len(fields))
	}
	for i := 0; i < cap(output); i++ {
		output = append(output, uint32(i))
	}
	dag.OutputOffsets = output
	return dag
}

func (t *Table) getLimitExec(count int) *tipb.Executor {
	exe := &tipb.Executor{}
	exe.Tp = tipb.ExecType_TypeLimit

	limit := &tipb.Limit{}
	limit.Limit = uint64(count)

	exe.Limit = limit

	return exe
}

func (t *Table) getTableScanExec(indexes []uint32) *tipb.Executor {
	exe := &tipb.Executor{}
	exe.Tp = tipb.ExecType_TypeTableScan

	scan := &tipb.TableScan{}
	scan.Desc = false
	scan.TableId = t.tableID
	scan.Columns = make([]*tipb.ColumnInfo, 0, len(indexes))
	for _, index := range indexes {
		scan.Columns = append(scan.Columns, &t.columns[index])
	}

	exe.TblScan = scan
	return exe
}

// GetPointRange returns kv.KeyRange, which is [key,PrefixNextKey).
func (t *Table) GetPointRange(key string) kv.KeyRange {
	startKey := tablecodec.EncodeRowKey(t.tableID, []byte(key))
	return kv.KeyRange{StartKey: startKey, EndKey: startKey.PrefixNext()}
}

// GetScanRange returns kv.KeyRange, the start of range is key,
// and the end of the range is the next table key.
func (t *Table) GetScanRange(key string) kv.KeyRange {
	startKey := tablecodec.EncodeRowKey(t.tableID, []byte(key))
	nextTableID := t.tableID + 1
	endKey := tablecodec.EncodeRowKeyWithHandle(nextTableID, math.MinInt64)
	return kv.KeyRange{StartKey: startKey, EndKey: endKey}
}

// EncodeKey encodes the key and tableId into a kv.Key.
func (t *Table) EncodeKey(key string) kv.Key {
	return tablecodec.EncodeRowKey(t.tableID, []byte(key))
}

// DecodeValue decodes the row with the specified fields, and returns the field-value map.
func (t *Table) DecodeValue(row []byte, field []string) (map[string][]byte, error) {
	return t.rowC.Decode(row, field)
}

// EncodeValue encodes the values.
func (t *Table) EncodeValue(buf []byte, values map[string][]byte) ([]byte, error) {
	return t.rowC.Encode(buf, values)
}
