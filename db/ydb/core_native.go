package ydb

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"time"
)

type driverNative struct {
	db  ydb.Connection
	dsn string
}

var (
	_ driverCore = (*driverNative)(nil)

	txControlReadWrite = table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
)

func (d *driverNative) close() error {
	return d.db.Close(context.Background())
}

func (d *driverNative) executeSchemeQuery(ctx context.Context, query string) error {
	return d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.ExecuteSchemeQuery(ctx, query)
	}, table.WithIdempotent())
}

func (d *driverNative) queryRows(ctx context.Context, query string, count int, params *table.QueryParameters) ([]map[string][]byte, error) {
	vs := make([]map[string][]byte, 0, count)

	err := d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		vs = vs[:0]

		var (
			rows result.BaseResult
			err  error
		)

		if count <= truncatedThreshold {
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

func (d *driverNative) executeDataQuery(ctx context.Context, query string, params *table.QueryParameters) error {
	return d.db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, _, err := s.Execute(ctx, txControlReadWrite, query, params)
		return err

	}, table.WithIdempotent())
}

func openNative(ctx context.Context, dsn string, limit int) (*driverNative, error) {
	db, err := ydb.Open(ctx, dsn,
		ydb.WithSessionPoolSizeLimit(limit+10),
		ydb.WithDialTimeout(time.Minute),
	)
	if err != nil {
		fmt.Printf("failed to open native ydb driver: %v", err)
		return nil, err
	}
	return &driverNative{
		db:  db,
		dsn: dsn,
	}, nil
}
