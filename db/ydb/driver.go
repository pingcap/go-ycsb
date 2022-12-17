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

package ydb

import (
	"context"
	"fmt"
	"hash/fnv"
	"math"
	"strings"

	"github.com/magiconair/properties"

	// ydb package
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	truncatedThreshold = 1000
)

type (
	driverCore interface {
		executeSchemeQuery(ctx context.Context, query string) error
		queryRows(ctx context.Context, query string, count int, params *table.QueryParameters) ([]map[string][]byte, error)
		executeDataQuery(ctx context.Context, query string, params *table.QueryParameters) error
		close() error
	}
	driver struct {
		p            *properties.Properties
		cores        []driverCore
		verbose      bool
		forceUpsert  bool
		useHash      bool
		buildersPool buildersPool
	}
	ctxThreadIDKey struct{}
)

var (
	_ ycsb.DB      = (*driver)(nil)
	_ ycsb.BatchDB = (*driver)(nil)
)

func (d *driver) calculateAvgRowSize() int64 {
	fieldCount := d.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := d.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	fieldLengthDistribution := d.p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)

	avgFieldLength := int64(0)
	switch fieldLengthDistribution {
	case "constant":
		avgFieldLength = fieldLength
	case "uniform":
		avgFieldLength = fieldLength/2 + 1
	case "zipfian":
		avgFieldLength = fieldLength/4 + 1
	case "histogram":
		avgFieldLength = fieldLength
	default:
		avgFieldLength = fieldLength
	}
	return avgFieldLength * fieldCount
}

func (d *driver) createTable() error {
	ctx := context.Background()

	tableName := d.p.GetString(prop.TableName, prop.TableNameDefault)

	if d.p.GetBool(prop.DropData, prop.DropDataDefault) {
		_ = d.cores[0].executeSchemeQuery(ctx, "DROP TABLE "+tableName)
	}

	fieldCount := d.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	doCompression := d.p.GetBool(ydbCompression, ydbCompressionDefault)

	var builder strings.Builder

	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tableName)
	builder.WriteString(" (")
	if d.useHash {
		builder.WriteString("hash Uint32 NOT NULL")
		if doCompression {
			builder.WriteString(" default")
		}
		builder.WriteString(",")
	}
	builder.WriteString(" id Text NOT NULL")

	if doCompression {
		builder.WriteString(" default")
	}

	for i := int64(0); i < fieldCount; i++ {
		builder.WriteString(fmt.Sprintf(", field%d Bytes", i))
		if doCompression {
			builder.WriteString(" default")
		}
	}

	builder.WriteString(", PRIMARY KEY (")
	if d.useHash {
		builder.WriteString("hash, ")
	}
	builder.WriteString("id)")

	if doCompression {
		builder.WriteString(`, FAMILY default ( DATA = "ssd", COMPRESSION = "lz4")`)
	}

	builder.WriteString(")")

	if d.p.GetBool(ydbAutoPartitioning, ydbAutoPartitioningDefault) {
		avgRowSize := d.calculateAvgRowSize()
		recordCount := d.p.GetInt64(prop.RecordCount, prop.RecordCountDefault)
		maxPartSizeMB := d.p.GetInt64(ydbMaxPartSizeMb, ydbMaxPartSizeMbDefault)
		maxShards := d.p.GetInt64(ydbMaxPartCount, ydbMaxPartCountDefault)

		approximateDataSize := avgRowSize * recordCount
		avgPartSizeMB := int64(math.Max(float64(approximateDataSize)/float64(maxShards)/1000000, 1))
		partSizeMB := int64(math.Min(float64(avgPartSizeMB), float64(maxPartSizeMB)))

		splitByLoad := d.p.GetBool(ydbSplitByLoad, ydbSplitByLoadDefault)
		splitBySize := d.p.GetBool(ydbSplitBySize, ydbSplitBySizeDefault)
		fmt.Printf("After partitioning for %d records with avg row size %d: "+
			"minParts=%d, maxParts=%d, partSize=%d MB, splitByLoad=%t, splitBySize=%t\n",
			recordCount, avgRowSize, maxShards, maxShards, partSizeMB, splitByLoad, splitBySize,
		)

		builder.WriteString(" WITH (")
		builder.WriteString(fmt.Sprintf("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d", maxShards))
		builder.WriteString(fmt.Sprintf(", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d", maxShards))
		if splitByLoad {
			builder.WriteString(", AUTO_PARTITIONING_BY_LOAD = ENABLED")
		}
		builder.WriteString(", AUTO_PARTITIONING_BY_SIZE = ENABLED")
		if splitBySize {
			builder.WriteString(fmt.Sprintf(", AUTO_PARTITIONING_PARTITION_SIZE_MB = %d", maxPartSizeMB))
		}
		builder.WriteString(")")
	}

	builder.WriteString(";")

	query := builder.String()

	if d.verbose {
		fmt.Println(strings.ReplaceAll(query, "\n", " "))
	}

	return d.cores[0].executeSchemeQuery(ctx, query)
}

func (d *driver) Close() error {
	for i := range d.cores {
		if err := d.cores[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (d *driver) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return context.WithValue(ctx, ctxThreadIDKey{}, threadID)
}

func (d *driver) CleanupThread(context.Context) {
}

var (
	txControlReadOnly = table.TxControl(table.BeginTx(table.WithSnapshotReadOnly()), table.CommitTx())
)

func (d *driver) queryRows(ctx context.Context, query string, count int, params *table.QueryParameters) (_ []map[string][]byte, err error) {
	defer func() {
		if err != nil {
			fmt.Printf("queryRows failed: %v\n", err)
		}
	}()
	if d.verbose {
		fmt.Println(strings.ReplaceAll(query, "\n", " "))
	}
	threadID, has := ctx.Value(ctxThreadIDKey{}).(int)
	if !has {
		return nil, fmt.Errorf("context not contains threadID identifier")
	}
	return d.cores[threadID%len(d.cores)].queryRows(ctx, query, count, params)
}

func (d *driver) Read(ctx context.Context, tableName string, id string, fields []string) (map[string][]byte, error) {
	var (
		builder = d.buildersPool.Get()
		params  = table.NewQueryParameters(
			table.ValueParam("$id", types.TextValue(id)),
		)
	)
	defer d.buildersPool.Put(builder)

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return nil, err
	}

	builder.WriteString(declares)

	builder.WriteString("SELECT ")
	if len(fields) == 0 {
		builder.WriteByte('*')
	} else {
		for i, field := range fields {
			if i != 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(field)
		}
	}
	builder.WriteString("\nFROM ")
	builder.WriteString(tableName)
	builder.WriteString("\nWHERE id = $id;")

	rows, err := d.queryRows(ctx, builder.String(), 1, params)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (d *driver) Scan(ctx context.Context, tableName string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var (
		builder = d.buildersPool.Get()
		params  = table.NewQueryParameters(
			table.ValueParam("$id", types.TextValue(startKey)),
			table.ValueParam("$limit", types.Uint64Value(uint64(count))),
		)
	)
	defer d.buildersPool.Put(builder)

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return nil, err
	}

	builder.WriteString(declares)

	builder.WriteString("SELECT ")
	if len(fields) == 0 {
		builder.WriteByte('*')
	} else {
		for i, field := range fields {
			if i != 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(field)
		}
	}
	builder.WriteString("\nFROM ")
	builder.WriteString(tableName)
	builder.WriteString("\nWHERE id >= $id LIMIT $limit;")

	rows, err := d.queryRows(ctx, builder.String(), count, params)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (d *driver) execQuery(ctx context.Context, query string, params *table.QueryParameters) (err error) {
	defer func() {
		if err != nil {
			fmt.Printf("execQuery failed: %v\n", err)
		}
	}()
	if d.verbose {
		fmt.Println(strings.ReplaceAll(query, "\n", " "))
	}
	threadID, has := ctx.Value(ctxThreadIDKey{}).(int)
	if !has {
		return fmt.Errorf("context not contains threadID identifier")
	}
	return d.cores[threadID%len(d.cores)].executeDataQuery(ctx, query, params)
}

func (d *driver) insertOrUpsert(ctx context.Context, op string, tableName string, id string, values map[string][]byte) error {
	var (
		paramOptions = make([]table.ParameterOption, 0, 1+len(values))
		pairs        = util.NewFieldPairs(values)
		builder      = d.buildersPool.Get()
	)
	defer d.buildersPool.Put(builder)

	if d.useHash {
		paramOptions = append(paramOptions,
			table.ValueParam("$hash", types.Uint32Value(d.hash(id))),
		)
	}
	paramOptions = append(paramOptions,
		table.ValueParam("$id", types.TextValue(id)),
	)
	for _, p := range pairs {
		paramOptions = append(paramOptions, table.ValueParam("$"+p.Field, types.BytesValue(p.Value)))
	}

	params := table.NewQueryParameters(paramOptions...)

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return err
	}

	builder.WriteString(declares)

	builder.WriteString(op)
	builder.WriteString(" INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" (")
	if d.useHash {
		builder.WriteString("hash, ")
	}
	builder.WriteString("id")
	for _, p := range pairs {
		builder.WriteByte(',')
		builder.WriteString(p.Field)
	}
	builder.WriteString(")\nVALUES (")
	if d.useHash {
		builder.WriteString("$hash, ")
	}
	builder.WriteString("$id")

	for _, p := range pairs {
		builder.WriteString(fmt.Sprintf(",$%s", p.Field))
	}

	builder.WriteString(");")

	return d.execQuery(ctx, builder.String(), params)
}

func (d *driver) Update(ctx context.Context, table string, id string, values map[string][]byte) error {
	return d.insertOrUpsert(ctx, "UPSERT", table, id, values)
}

func (d *driver) Insert(ctx context.Context, table string, id string, values map[string][]byte) error {
	if d.forceUpsert {
		return d.insertOrUpsert(ctx, "UPSERT", table, id, values)
	}
	return d.insertOrUpsert(ctx, "INSERT", table, id, values)
}

func (d *driver) Delete(ctx context.Context, tableName string, id string) error {
	builder := d.buildersPool.Get()
	defer d.buildersPool.Put(builder)

	params := table.NewQueryParameters(table.ValueParam("$id", types.TextValue(id)))

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return err
	}

	builder.WriteString(declares)

	builder.WriteString("DELETE FROM ")
	builder.WriteString(tableName)
	builder.WriteString(" WHERE id = $id;")

	return d.execQuery(ctx, builder.String(), table.NewQueryParameters(
		table.ValueParam("$id", types.TextValue(id)),
	))
}

func (d *driver) hash(s string) uint32 {
	v := fnv.New32a()
	v.Write([]byte(s))
	return v.Sum32()
}

func (d *driver) batchInsertOrUpsert(ctx context.Context, op string, tableName string, ids []string, values []map[string][]byte) error {
	var (
		builder = d.buildersPool.Get()
		pairs   = util.NewFieldPairs(values[0])
	)
	defer d.buildersPool.Put(builder)

	rows := make([]types.Value, 0, len(ids))
	for i, rowValues := range values {
		row := make([]types.StructValueOption, 0, 1+len(rowValues))
		if d.useHash {
			row = append(row,
				types.StructFieldValue("hash", types.Uint32Value(d.hash(ids[i]))),
			)
		}
		row = append(row,
			types.StructFieldValue("id", types.TextValue(ids[i])),
		)
		for _, field := range pairs {
			row = append(row, types.StructFieldValue(field.Field, types.BytesValue(field.Value)))
		}
		rows = append(rows, types.StructValue(row...))
	}

	params := table.NewQueryParameters(table.ValueParam("$values", types.ListValue(rows...)))

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return err
	}

	builder.WriteString(declares)

	builder.WriteString(op)
	builder.WriteString(" INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" SELECT * FROM AS_TABLE($values);")

	return d.execQuery(ctx, builder.String(), params)
}

func (d *driver) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	if d.forceUpsert {
		return d.batchInsertOrUpsert(ctx, "UPSERT", table, keys, values)
	}
	return d.batchInsertOrUpsert(ctx, "INSERT", table, keys, values)
}

func (d *driver) BatchRead(ctx context.Context, tableName string, keys []string, fields []string) ([]map[string][]byte, error) {
	var (
		builder = d.buildersPool.Get()
		ids     = make([]types.Value, len(keys))
	)
	defer d.buildersPool.Put(builder)

	for i, key := range keys {
		ids[i] = types.TextValue(key)
	}

	params := table.NewQueryParameters(table.ValueParam("$ids", types.ListValue(ids...)))

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return nil, err
	}

	builder.WriteString(declares)

	builder.WriteString("SELECT ")
	if len(fields) == 0 {
		builder.WriteByte('*')
	} else {
		for i, field := range fields {
			if i != 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(field)
		}
	}
	builder.WriteString("\nFROM ")
	builder.WriteString(tableName)
	builder.WriteString(" WHERE id IN $ids;")

	return d.queryRows(ctx, builder.String(), len(ids), params)
}

func (d *driver) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return d.batchInsertOrUpsert(ctx, "UPSERT", table, keys, values)
}

func (d *driver) BatchDelete(ctx context.Context, tableName string, keys []string) error {
	var (
		builder = d.buildersPool.Get()
		ids     = make([]types.Value, len(keys))
	)
	defer d.buildersPool.Put(builder)

	for i, key := range keys {
		ids[i] = types.TextValue(key)
	}

	params := table.NewQueryParameters(table.ValueParam("$ids", types.ListValue(ids...)))

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return err
	}

	builder.WriteString(declares)

	builder.WriteString("DELETE FROM ")
	builder.WriteString(tableName)
	builder.WriteString(" WHERE ids IN $ids;")

	return d.execQuery(ctx, builder.String(), params)
}
