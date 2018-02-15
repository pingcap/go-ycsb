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
	"context"
	"database/sql"
	"fmt"

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
)

type mysqlCreator struct {
}

type mysqlDB struct {
	db *sql.DB
}

func (c mysqlCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	d := new(mysqlDB)
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

	d.db = db

	return d, nil
}

func (db mysqlDB) Close() error {
	return db.db.Close()
}

func (db mysqlDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db mysqlDB) CleanupThread(_ context.Context) {

}

func (db mysqlDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	return nil, nil
}

func (db mysqlDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, nil
}

func (db mysqlDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

func (db mysqlDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	return nil
}

func (db mysqlDB) Delete(ctx context.Context, table string, key string) error {
	return nil
}

func init() {
	ycsb.RegisterDBCreator("mysql", mysqlCreator{})
}
