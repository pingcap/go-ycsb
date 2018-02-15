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

package main

import (
	"context"
	"time"

	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type dbWrapper struct {
	ycsb.DB
}

func (db dbWrapper) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	start := time.Now()
	defer func() {
		measurement.Measure("READ", time.Now().Sub(start))
	}()

	return db.DB.Read(ctx, table, key, fields)
}

func (db dbWrapper) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	start := time.Now()
	defer func() {
		measurement.Measure("SCAN", time.Now().Sub(start))
	}()

	return db.DB.Scan(ctx, table, startKey, count, fields)
}

func (db dbWrapper) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	defer func() {
		measurement.Measure("UPDATE", time.Now().Sub(start))
	}()

	return db.DB.Update(ctx, table, key, values)
}

func (db dbWrapper) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	start := time.Now()
	defer func() {
		measurement.Measure("INSERT", time.Now().Sub(start))
	}()

	return db.DB.Insert(ctx, table, key, values)
}

func (db dbWrapper) Delete(ctx context.Context, table string, key string) error {
	start := time.Now()
	defer func() {
		measurement.Measure("DELETE", time.Now().Sub(start))
	}()

	return db.DB.Delete(ctx, table, key)
}
