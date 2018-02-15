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

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/go-ycsb/pkg/prop"

	// mysql package
	_ "github.com/go-sql-driver/mysql"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// mysql properties
const (
	mysqlHost     = "mysql.host"
	mysqlPort     = "mysql.port"
	mysqlUser     = "mysql.user"
	mysqlPassword = "mysql.password"
	mysqlDBName   = "mysql.db"
	mysqlVerbose  = "mysql.verbose"

	// TODO: support batch and auto commit
)

type mysqlCreator struct {
}

type mysqlDB struct {
	p       *properties.Properties
	db      *sql.DB
	verbose bool
	// TODO: support caching prepare statement
}

func (c mysqlCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(mysqlDB)
	d.p = p

	host := p.GetString(mysqlHost, "127.0.0.1")
	port := p.GetInt(mysqlPort, 3306)
	user := p.GetString(mysqlUser, "root")
	password := p.GetString(mysqlPassword, "")
	dbName := p.GetString(mysqlDBName, "test")

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbName)
	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	db.SetMaxIdleConns(threadCount + 1)
	db.SetMaxOpenConns(threadCount * 2)

	d.verbose = p.GetBool(mysqlVerbose, false)
	d.db = db

	if err := d.createTable(); err != nil {
		return nil, err
	}

	return d, nil
}

func (db *mysqlDB) createTable() error {
	tableName := db.p.GetString(prop.TableName, prop.TableNameDefault)

	if _, err := db.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
		return err
	}

	fieldCount := db.p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	fieldLength := db.p.GetInt64(prop.FieldLength, prop.FieldLengthDefault)

	buf := new(bytes.Buffer)
	s := fmt.Sprintf("CREATE TABLE %s (YCSB_KEY VARCHAR(%d) PRIMARY KEY", tableName, fieldLength)
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

func (db *mysqlDB) Close() error {
	if db.db == nil {
		return nil
	}

	return db.db.Close()
}

func (db *mysqlDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *mysqlDB) CleanupThread(_ context.Context) {

}

func (db *mysqlDB) queryRows(ctx context.Context, query string, count int) ([]map[string][]byte, error) {
	rows, err := db.db.QueryContext(ctx, query)
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

func (db *mysqlDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY = "%s"`, table, key)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY = "%s"`, strings.Join(fields, ","), table, key)
	}

	if db.verbose {
		fmt.Println(query)
	}

	rows, err := db.queryRows(ctx, query, 1)
	if err != nil {
		return nil, err
	} else if len(rows) == 0 {
		return nil, nil
	}

	return rows[0], nil
}

func (db *mysqlDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE YCSB_KEY >= "%s" LIMIT %d`, table, startKey, count)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE YCSB_KEY >= "%s" LIMIT %d`, strings.Join(fields, ","), table, startKey, count)
	}

	if db.verbose {
		fmt.Println(query)
	}

	rows, err := db.queryRows(ctx, query, count)
	return rows, err
}

func (db *mysqlDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	var query string
	buf := new(bytes.Buffer)
	firstField := true
	for field, value := range values {
		if !firstField {
			buf.WriteString(", ")
		}

		buf.WriteString(field)
		buf.WriteString(`= "`)
		buf.Write(value)
		buf.WriteString(`"`)
	}

	// TODO: use escape or prepare statement
	query = fmt.Sprintf(`UPDATE %s SET %s WHERE YCSB_KEY = "%s"`, table, buf.Bytes(), key)

	if db.verbose {
		fmt.Println(query)
	}

	_, err := db.db.ExecContext(ctx, query)
	return err
}

func (db *mysqlDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	fields := make([]string, 0, 1+len(values))
	vs := make([][]byte, 0, 1+len(values))

	fields = append(fields, "YCSB_KEY")
	vs = append(vs, []byte(key))

	for field, value := range values {
		fields = append(fields, field)
		vs = append(vs, value)
	}

	// TODO: use escape or prepare statement
	query := fmt.Sprintf(`INSERT IGNORE INTO %s (%s) VALUES ("%s")`, table, strings.Join(fields, ", "), bytes.Join(vs, []byte(`", "`)))

	if db.verbose {
		fmt.Println(query)
	}

	_, err := db.db.ExecContext(ctx, query)
	return err
}

func (db *mysqlDB) Delete(ctx context.Context, table string, key string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE YCSB_KEY = "%s"`, table, key)

	if db.verbose {
		fmt.Println(query)
	}

	_, err := db.db.ExecContext(ctx, query)
	return err
}

func init() {
	ycsb.RegisterDBCreator("mysql", mysqlCreator{})
}
