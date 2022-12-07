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
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"

	// ydb package
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// ydb properties
const (
	ydbDSN              = "ydb.dsn"
	ydbAutoPartitioning = "ydb.auto.partitioning"
	ydbMaxPartSizeMb    = "ydb.max.part.size.mb"
	ydbMaxPartCount     = "ydb.max.parts.count"
	ydbSplitByLoad      = "ydb.split.by.load"
	ydbSplitBySize      = "ydb.split.by.size"
	ydbForceUpsert      = "ydb.force.upsert"

	maxPartitionsSize  = int64(2000) // 2 GB
	maxPartitionsCount = int64(50)
)

type ydbCreator struct {
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

type ydbDB struct {
	p           *properties.Properties
	db          *sql.DB
	verbose     bool
	forceUpsert bool

	buildersPool buildersPool
}

func (db *ydbDB) calculateAvgRowSize() int64 {
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

func (c ydbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := &ydbDB{
		p: p,
	}
	d.p = p

	dsn := p.GetString(ydbDSN, "grpc://localhost:2136/local")

	db, err := sql.Open("ydb", dsn)
	if err != nil {
		fmt.Printf("open ydb failed %v", err)
		return nil, err
	}

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxIdleConns(threadCount + 1)
	db.SetMaxOpenConns(threadCount * 2)

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.forceUpsert = p.GetBool(ydbForceUpsert, false)
	d.db = db

	if err = d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *ydbDB) createTable() error {
	ctx := ydb.WithQueryMode(context.Background(), ydb.SchemeQueryMode)

	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if db.p.GetBool(prop.DropData, prop.DropDataDefault) {
		_ = retry.Do(ctx, db.db, func(ctx context.Context, cc *sql.Conn) error {
			_, err := db.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s", tableName))
			return err
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)

	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("CREATE TABLE %s (id Text NOT NULL", tableName))

	for i := int64(0); i < fieldCount; i++ {
		builder.WriteString(fmt.Sprintf(", field%d Bytes", i))
	}

	builder.WriteString(", PRIMARY KEY (id)")
	builder.WriteString(")")

	if db.p.GetBool(ydbAutoPartitioning, true) {
		avgRowSize := db.calculateAvgRowSize()
		recordCount := db.p.GetInt64(prop.RecordCount, prop.RecordCountDefault)
		maxPartSizeMB := db.p.GetInt64(ydbMaxPartSizeMb, maxPartitionsSize)
		maxParts := db.p.GetInt64(ydbMaxPartCount, maxPartitionsCount)
		minParts := maxParts

		approximateDataSize := avgRowSize * recordCount
		avgPartSizeMB := int64(math.Max(float64(approximateDataSize)/float64(maxParts)/1000000, 1))
		partSizeMB := int64(math.Min(float64(avgPartSizeMB), float64(maxPartSizeMB)))

		splitByLoad := db.p.GetBool(ydbSplitByLoad, true)
		splitBySize := db.p.GetBool(ydbSplitBySize, true)
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

	return retry.Do(ctx, db.db, func(ctx context.Context, cc *sql.Conn) error {
		_, err := db.db.ExecContext(ctx, builder.String())
		return err
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
}

func (db *ydbDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *ydbDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *ydbDB) CleanupThread(ctx context.Context) {
}

func (db *ydbDB) queryRows(ctx context.Context, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	vs := make([]map[string][]byte, 0, count)

	err := retry.Do(ctx, db.db, func(ctx context.Context, cc *sql.Conn) error {
		vs = vs[:0]

		rows, err := cc.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return err
		}

		for rows.Next() {
			m := make(map[string][]byte, len(cols))
			dest := make([]interface{}, len(cols))
			for i := 0; i < len(cols); i++ {
				v := new([]byte)
				dest[i] = v
			}
			if err = rows.Scan(dest...); err != nil {
				return err
			}

			for i, v := range dest {
				m[cols[i]] = *v.(*[]byte)
			}

			vs = append(vs, m)
		}

		return rows.Err()
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

	return vs, err
}

func (db *ydbDB) Read(ctx context.Context, table string, id string, fields []string) (map[string][]byte, error) {
	builder := db.buildersPool.Get()
	defer db.buildersPool.Put(builder)

	builder.WriteString("DECLARE $id AS Text;\n\nSELECT ")
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
	builder.WriteString(table)
	builder.WriteString("\nWHERE id = $id;")

	rows, err := db.queryRows(ctx, builder.String(), 1, sql.Named("id", id))
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *ydbDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	builder := db.buildersPool.Get()
	defer db.buildersPool.Put(builder)

	builder.WriteString("DECLARE $id AS Text;\nDECLARE $limit AS Uint64;\n\nSELECT ")
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
	builder.WriteString(table)
	builder.WriteString("\nWHERE id >= $id LIMIT $limit;")

	rows, err := db.queryRows(ctx, builder.String(), count, sql.Named("id", startKey), sql.Named("limit", count))
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (db *ydbDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	return retry.Do(ctx, db.db, func(ctx context.Context, cc *sql.Conn) error {
		_, err := cc.ExecContext(ctx, query, args...)
		return err
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
}

func (db *ydbDB) insertOrUpsert(ctx context.Context, op string, table string, id string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, sql.Named("id", id))

	builder := db.buildersPool.Get()
	defer db.buildersPool.Put(builder)

	builder.WriteString("DECLARE $id AS Text;\n")
	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, sql.Named(p.Field, p.Value))
		builder.WriteString("DECLARE $")
		builder.WriteString(p.Field)
		builder.WriteString(" AS Bytes;\n")
	}

	builder.WriteString("\n")
	builder.WriteString(op)
	builder.WriteString(" INTO ")
	builder.WriteString(table)
	builder.WriteString(" (id")
	for _, p := range pairs {
		builder.WriteString(" ,")
		builder.WriteString(p.Field)
	}
	builder.WriteString(")\nVALUES ($id")

	for _, p := range pairs {
		builder.WriteString(fmt.Sprintf(" ,$%s", p.Field))
	}

	builder.WriteString(");")

	return db.execQuery(ctx, builder.String(), args...)
}

func (db *ydbDB) Update(ctx context.Context, table string, id string, values map[string][]byte) error {
	return db.insertOrUpsert(ctx, "UPSERT", table, id, values)
}

func (db *ydbDB) Insert(ctx context.Context, table string, id string, values map[string][]byte) error {
	if db.forceUpsert {
		return db.insertOrUpsert(ctx, "UPSERT", table, id, values)
	}
	return db.insertOrUpsert(ctx, "INSERT", table, id, values)
}

func (db *ydbDB) Delete(ctx context.Context, table string, id string) error {
	query := fmt.Sprintf(`DECLARE $id AS Text; DELETE FROM %s WHERE id = $id`, table)

	return db.execQuery(ctx, query, sql.Named("id", id))
}

func init() {
	ycsb.RegisterDBCreator("ydb", ydbCreator{})
}
