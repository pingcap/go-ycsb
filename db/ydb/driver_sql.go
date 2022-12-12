package ydb

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type driverSql struct {
	db *sql.DB
}

var (
	_ driverCore = (*driverSql)(nil)
)

func (d *driverSql) close() error {
	return d.db.Close()
}

func (d *driverSql) queryRows(ctx context.Context, query string, count int, params *table.QueryParameters) ([]map[string][]byte, error) {
	vs := make([]map[string][]byte, 0, count)

	err := retry.Do(ctx, d.db, func(ctx context.Context, cc *sql.Conn) error {
		vs = vs[:0]

		if count > truncatedThreshold {
			ctx = ydb.WithQueryMode(ctx, ydb.ScanQueryMode)
		}

		rows, err := cc.QueryContext(ctx, query, params)
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

func (d *driverSql) executeDataQuery(ctx context.Context, query string, params *table.QueryParameters) error {
	return retry.Do(ctx, d.db, func(ctx context.Context, cc *sql.Conn) error {
		_, err := cc.ExecContext(ctx, query, params)
		return err
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
}

func (d *driverSql) executeSchemeQuery(ctx context.Context, query string) error {
	return retry.Do(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), d.db,
		func(ctx context.Context, cc *sql.Conn) error {
			_, err := cc.ExecContext(ctx, query)
			return err
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
	)
}

func openSql(ctx context.Context, dsn string, limit int) (*driverSql, error) {
	db, err := sql.Open("ydb", dsn)
	if err != nil {
		fmt.Printf("failed to open database/sql ydb driver: %v", err)
		return nil, err
	}
	db.SetMaxIdleConns(limit + 1)
	db.SetMaxOpenConns(limit * 2)
	go func() {
		for {
			fmt.Printf("Stats: %+v\n", db.Stats())
			time.Sleep(time.Second)
		}
	}()
	return &driverSql{
		db: db,
	}, db.PingContext(ctx)
}
