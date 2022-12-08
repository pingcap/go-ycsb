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
	"math"
	"strings"
	"sync"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	// ydb package
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// ydb properties
const (
	ydbDSN        = "ydb.dsn"
	ydbDSNDefault = "grpc://localhost:2136/local"

	ydbCompression        = "ydb.compression"
	ydbCompressionDefault = false

	ydbAutoPartitioning        = "ydb.auto.partitioning"
	ydbAutoPartitioningDefault = true

	ydbMaxPartSizeMb        = "ydb.max.part.size.mb"
	ydbMaxPartSizeMbDefault = int64(2000) // 2GB

	ydbMaxPartCount        = "ydb.max.parts.count"
	ydbMaxPartCountDefault = int64(50)

	ydbSplitByLoad        = "ydb.split.by.load"
	ydbSplitByLoadDefault = true

	ydbSplitBySize        = "ydb.split.by.size"
	ydbSplitBySizeDefault = true

	ydbForceUpsert        = "ydb.force.upsert"
	ydbForceUpsertDefault = false

	ydbUntruncatedLimit = 1000
)

type creator struct {
}

type buildersPool struct {
	sync.Pool
}

func (p buildersPool) Get() *strings.Builder {
	v := p.Pool.Get()
	if v == nil {
		v = new(strings.Builder)
	}
	return v.(*strings.Builder)
}

func (p buildersPool) Put(b *strings.Builder) {
	b.Reset()
	p.Pool.Put(b)
}

type db struct {
	p           *properties.Properties
	db          ydb.Connection
	verbose     bool
	forceUpsert bool

	buildersPool buildersPool
}

var (
	_ ycsb.DB      = (*db)(nil)
	_ ycsb.BatchDB = (*db)(nil)
)

func (db *db) calculateAvgRowSize() int64 {
	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)
	fieldLengthDistribution := db.p.GetString(prop.FieldLengthDistribution, prop.FieldLengthDistributionDefault)

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

func (c creator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := &db{
		p: p,
	}
	d.p = p

	dsn := p.GetString(ydbDSN, ydbDSNDefault)

	ctx := context.Background()

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db, err := ydb.Open(ctx, dsn,
		ydb.WithSessionPoolSizeLimit(threadCount+10),
	)
	if err != nil {
		fmt.Printf("failed to open ydb: %v", err)
		return nil, err
	}

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.forceUpsert = p.GetBool(ydbForceUpsert, ydbForceUpsertDefault)
	d.db = db

	if err = d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *db) createTable() error {
	ctx := context.Background()

	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		_ = db.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
			return s.ExecuteSchemeQuery(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
		}, table.WithIdempotent())
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	doCompression := db.p.GetBool(ydbCompression, ydbCompressionDefault)

	var builder strings.Builder

	builder.WriteString("CREATE TABLE ")
	builder.WriteString(tableName)
	builder.WriteString(" (id Text NOT NULL")

	if doCompression {
		builder.WriteString(" default")
	}

	for i := int64(0); i < fieldCount; i++ {
		builder.WriteString(fmt.Sprintf(", field%d Bytes", i))
		if doCompression {
			builder.WriteString(" default")
		}
	}

	builder.WriteString(", PRIMARY KEY (id)")

	if doCompression {
		builder.WriteString(`, FAMILY default ( DATA = "ssd", COMPRESSION = "lz4")`)
	}

	builder.WriteString(")")

	if db.p.GetBool(ydbAutoPartitioning, ydbAutoPartitioningDefault) {
		avgRowSize := db.calculateAvgRowSize()
		recordCount := db.p.GetInt64(prop.RecordCount, prop.RecordCountDefault)
		maxPartSizeMB := db.p.GetInt64(ydbMaxPartSizeMb, ydbMaxPartSizeMbDefault)
		maxParts := db.p.GetInt64(ydbMaxPartCount, ydbMaxPartCountDefault)
		minParts := maxParts

		approximateDataSize := avgRowSize * recordCount
		avgPartSizeMB := int64(math.Max(float64(approximateDataSize)/float64(maxParts)/1000000, 1))
		partSizeMB := int64(math.Min(float64(avgPartSizeMB), float64(maxPartSizeMB)))

		splitByLoad := db.p.GetBool(ydbSplitByLoad, ydbSplitByLoadDefault)
		splitBySize := db.p.GetBool(ydbSplitBySize, ydbSplitBySizeDefault)
		fmt.Printf("After partitioning for %d records with avg row size %d: "+
			"minParts=%d, maxParts=%d, partSize=%d MB, splitByLoad=%t, splitBySize=%t\n",
			recordCount, avgRowSize, minParts, maxParts, partSizeMB, splitByLoad, splitBySize,
		)

		builder.WriteString(" WITH (")
		builder.WriteString(fmt.Sprintf("AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d", minParts))
		builder.WriteString(fmt.Sprintf(", AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = %d", maxParts))
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

	if db.verbose {
		fmt.Println(strings.ReplaceAll(query, "\n", " "))
	}

	return db.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
	}, table.WithIdempotent())
}

func (db *db) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close(context.Background())
}

func (db *db) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *db) CleanupThread(ctx context.Context) {
}

var (
	txControlReadOnly = table.TxControl(table.BeginTx(table.WithSnapshotReadOnly()), table.CommitTx())
)

func (db *db) queryRows(ctx context.Context, query string, count int, params *table.QueryParameters) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Println(strings.ReplaceAll(query, "\n", " "))
	}

	vs := make([]map[string][]byte, 0, count)

	err := db.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		vs = vs[:0]

		var (
			rows result.BaseResult
			err  error
		)

		if count <= ydbUntruncatedLimit {
			_, rows, err = s.Execute(ctx, txControlReadOnly, query, params, options.WithKeepInCache(true))
		} else {
			rows, err = s.StreamExecuteScanQuery(ctx, query, params)
		}

		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.NextResultSet(ctx) {
			resultSet := rows.CurrentResultSet()

			for rows.NextRow() {
				m := make(map[string][]byte, resultSet.ColumnCount())
				b := make([][]byte, resultSet.ColumnCount())

				values := make([]named.Value, 0, resultSet.ColumnCount())
				resultSet.Columns(func(column options.Column) {
					values = append(values, named.OptionalWithDefault(column.Name, &b[len(values)]))
				})

				if err = rows.ScanNamed(values...); err != nil {
					return err
				}

				for i, v := range values {
					m[v.Name] = b[i]
				}

				vs = append(vs, m)
			}
		}

		return rows.Err()
	}, table.WithIdempotent())

	return vs, err
}

func (db *db) Read(ctx context.Context, tableName string, id string, fields []string) (map[string][]byte, error) {
	var (
		builder = db.buildersPool.Get()
		params  = table.NewQueryParameters(
			table.ValueParam("$id", types.TextValue(id)),
		)
	)
	defer db.buildersPool.Put(builder)

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

	rows, err := db.queryRows(ctx, builder.String(), 1, params)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *db) Scan(ctx context.Context, tableName string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var (
		builder = db.buildersPool.Get()
		params  = table.NewQueryParameters(
			table.ValueParam("$id", types.TextValue(startKey)),
			table.ValueParam("$limit", types.Uint64Value(uint64(count))),
		)
	)
	defer db.buildersPool.Put(builder)

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

	rows, err := db.queryRows(ctx, builder.String(), count, params)
	if err != nil {
		return nil, err
	}

	return rows, nil
}

var (
	txControlReadWrite = table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
)

func (db *db) execQuery(ctx context.Context, query string, params *table.QueryParameters) error {
	if db.verbose {
		fmt.Println(strings.ReplaceAll(query, "\n", " "))
	}

	return db.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, _, err := s.Execute(ctx, txControlReadWrite, query, params)
		return err

	}, table.WithIdempotent())
}

func (db *db) insertOrUpsert(ctx context.Context, op string, tableName string, id string, values map[string][]byte) error {
	var (
		paramOptions = make([]table.ParameterOption, 0, 1+len(values))
		pairs        = util.NewFieldPairs(values)
		builder      = db.buildersPool.Get()
	)
	defer db.buildersPool.Put(builder)

	paramOptions = append(paramOptions, table.ValueParam("$id", types.TextValue(id)))
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
	builder.WriteString(" (id")
	for _, p := range pairs {
		builder.WriteByte(',')
		builder.WriteString(p.Field)
	}
	builder.WriteString(")\nVALUES ($id")

	for _, p := range pairs {
		builder.WriteString(fmt.Sprintf(",$%s", p.Field))
	}

	builder.WriteString(");")

	return db.execQuery(ctx, builder.String(), params)
}

func (db *db) Update(ctx context.Context, table string, id string, values map[string][]byte) error {
	return db.insertOrUpsert(ctx, "UPSERT", table, id, values)
}

func (db *db) Insert(ctx context.Context, table string, id string, values map[string][]byte) error {
	if db.forceUpsert {
		return db.insertOrUpsert(ctx, "UPSERT", table, id, values)
	}
	return db.insertOrUpsert(ctx, "INSERT", table, id, values)
}

func (db *db) Delete(ctx context.Context, tableName string, id string) error {
	builder := db.buildersPool.Get()
	defer db.buildersPool.Put(builder)

	params := table.NewQueryParameters(table.ValueParam("$id", types.TextValue(id)))

	declares, err := sugar.GenerateDeclareSection(params)
	if err != nil {
		return err
	}

	builder.WriteString(declares)

	builder.WriteString("DELETE FROM ")
	builder.WriteString(tableName)
	builder.WriteString(" WHERE id = $id;")

	return db.execQuery(ctx, builder.String(), table.NewQueryParameters(
		table.ValueParam("$id", types.TextValue(id)),
	))
}

func (db *db) batchInsertOrUpsert(ctx context.Context, op string, tableName string, ids []string, values []map[string][]byte) error {
	var (
		builder = db.buildersPool.Get()
		pairs   = util.NewFieldPairs(values[0])
	)
	defer db.buildersPool.Put(builder)

	rows := make([]types.Value, 0, len(ids))
	for i, rowValues := range values {
		row := make([]types.StructValueOption, 0, 1+len(rowValues))
		row = append(row, types.StructFieldValue("id", types.TextValue(ids[i])))
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

	return db.execQuery(ctx, builder.String(), params)
}

func (db *db) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	if db.forceUpsert {
		return db.batchInsertOrUpsert(ctx, "UPSERT", table, keys, values)
	}
	return db.batchInsertOrUpsert(ctx, "INSERT", table, keys, values)
}

func (db *db) BatchRead(ctx context.Context, tableName string, keys []string, fields []string) ([]map[string][]byte, error) {
	var (
		builder = db.buildersPool.Get()
		ids     = make([]types.Value, len(keys))
	)
	defer db.buildersPool.Put(builder)

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

	return db.queryRows(ctx, builder.String(), len(ids), params)
}

func (db *db) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return db.batchInsertOrUpsert(ctx, "UPSERT", table, keys, values)
}

func (db *db) BatchDelete(ctx context.Context, tableName string, keys []string) error {
	var (
		builder = db.buildersPool.Get()
		ids     = make([]types.Value, len(keys))
	)
	defer db.buildersPool.Put(builder)

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

	return db.execQuery(ctx, builder.String(), params)
}

func init() {
	ycsb.RegisterDBCreator("ydb", creator{})
}
