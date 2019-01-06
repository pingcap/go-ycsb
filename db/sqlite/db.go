// Copyright 2019 PingCAP, Inc.
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

package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/magiconair/properties"
	// sqlite package
	_ "github.com/mattn/go-sqlite3"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// Sqlite properties
const (
	sqliteDBPath      = "sqlite.db"
	sqliteMode        = "sqlite.mode"
	sqliteJournalMode = "sqlite.journalmode"
	sqliteCache       = "sqlite.cache"
)

type sqliteCreator struct {
}

type sqliteDB struct {
	p       *properties.Properties
	db      *sql.DB
	verbose bool

	bufPool *util.BufPool
}

type contextKey string

const stateKey = contextKey("sqliteDB")

type sqliteState struct {
	// Do we need a LRU cache here?
	stmtCache map[string]*sql.Stmt
}

func (c sqliteCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(sqliteDB)
	d.p = p

	dbPath := p.GetString(sqliteDBPath, "/tmp/sqlite.db")

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(dbPath)
	}

	mode := p.GetString(sqliteMode, "rwc")
	journalMode := p.GetString(sqliteJournalMode, "WAL")
	cache := p.GetString(sqliteCache, "shared")

	v := url.Values{}
	v.Set("cache", cache)
	v.Set("mode", mode)
	v.Set("_journal_mode", journalMode)
	dsn := fmt.Sprintf("file:%s?%s", dbPath, v.Encode())
	var err error
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)

	d.verbose = p.GetBool(prop.Verbose, prop.VerboseDefault)
	d.db = db

	d.bufPool = util.NewBufPool()

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *sqliteDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (YCSB_KEY VARCHAR(64) PRIMARY KEY", tableName)
	buf.WriteString(s)

	for i := int64(0); i < fieldCount; i++ {
		buf.WriteString(fmt.Sprintf(", FIELD%d VARCHAR(%d)", i, fieldLength))
	}

	buf.WriteString(");")

	if db.verbose {
		fmt.Println(buf.String())
	}

	_, err := db.db.Exec(buf.String())
	return err
}

func (db *sqliteDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *sqliteDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := &sqliteState{
		stmtCache: make(map[string]*sql.Stmt),
	}

	return context.WithValue(ctx, stateKey, state)
}

func (db *sqliteDB) CleanupThread(ctx context.Context) {
	state := ctx.Value(stateKey).(*sqliteState)

	for _, stmt := range state.stmtCache {
		stmt.Close()
	}
}

func (db *sqliteDB) getAndCacheStmt(ctx context.Context, query string) (*sql.Stmt, error) {
	state := ctx.Value(stateKey).(*sqliteState)

	if stmt, ok := state.stmtCache[query]; ok {
		return stmt, nil
	}

	stmt, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	state.stmtCache[query] = stmt
	return stmt, nil
}

func (db *sqliteDB) clearCacheIfFailed(ctx context.Context, query string, err error) {
	if err == nil {
		return
	}

	state := ctx.Value(stateKey).(*sqliteState)
	delete(state.stmtCache, query)
}

func (db *sqliteDB) queryRows(ctx context.Context, query string, count int, args ...interface{}) ([]map[string][]byte, error) {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	vs := make([]map[string][]byte, 0, count)
	for rows.Next() {
		m := make(map[string][]byte, len(cols))
		dest := make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			v := new([]byte)
			dest[i] = v
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}

		for i, v := range dest {
			m[cols[i]] = *v.(*[]byte)
		}

		vs = append(vs, m)
	}

	return vs, rows.Err()
}

func (db *sqliteDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = ?`, table)
	} else {
		sort.Strings(fields)
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = ?`, strings.Join(fields, ","), table)
	}

	rows, err := db.queryRows(ctx, query, 1, key)
	db.clearCacheIfFailed(ctx, query, err)

	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *sqliteDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= ? LIMIT ?`, table)
	} else {
		sort.Strings(fields)
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= ? LIMIT ?`, strings.Join(fields, ","), table)
	}

	rows, err := db.queryRows(ctx, query, count, startKey, count)
	db.clearCacheIfFailed(ctx, query, err)

	return rows, err
}

func (db *sqliteDB) execQuery(ctx context.Context, query string, args ...interface{}) error {
	if db.verbose {
		fmt.Printf("%s %v\n", query, args)
	}

	stmt, err := db.getAndCacheStmt(ctx, query)
	if err != nil {
		return err
	}

	_, err = stmt.ExecContext(ctx, args...)
	db.clearCacheIfFailed(ctx, query, err)
	return err
}

func (db *sqliteDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf.WriteString("UPDATE ")
	buf.WriteString(table)
	buf.WriteString(" SET ")
	firstField := true
	pairs := util.NewFieldPairs(values)
	args := make([]interface{}, 0, len(values)+1)
	for _, p := range pairs {
		if !firstField {
			buf.WriteString(", ")
		}

		buf.WriteString(p.Field)
		buf.WriteString(`= ?`)
		args = append(args, p.Value)
	}
	buf.WriteString(" WHERE YCSB_KEY = ?")

	args = append(args, key)

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *sqliteDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	args := make([]interface{}, 0, 1+len(values))
	args = append(args, key)

	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	buf.WriteString("INSERT OR IGNORE INTO ")
	buf.WriteString(table)
	buf.WriteString(" (YCSB_KEY")

	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		args = append(args, p.Value)
		buf.WriteString(" ,")
		buf.WriteString(p.Field)
	}
	buf.WriteString(") VALUES (?")

	for i := 0; i < len(pairs); i++ {
		buf.WriteString(" ,?")
	}

	buf.WriteByte(')')

	return db.execQuery(ctx, buf.String(), args...)
}

func (db *sqliteDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = ?`, table)

	return db.execQuery(ctx, query, key)
}

func init() {
	ycsb.RegisterDBCreator("sqlite", sqliteCreator{})
}
