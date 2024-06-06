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

/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package boltdb

import (
	"context"
	"fmt"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"go.etcd.io/bbolt"
	"os"
)

// properties
const (
	bboltPath            = "bbolt.path"
	bboltTimeout         = "bbolt.timeout"
	bboltNoGrowSync      = "bbolt.no_grow_sync"
	bboltReadOnly        = "bbolt.read_only"
	bboltMmapFlags       = "bbolt.mmap_flags"
	bboltInitialMmapSize = "bbolt.initial_mmap_size"
	bboltFreelistType    = "bbolt.freelist_type"
)

type bboltCreator struct {
}

type bboltOptions struct {
	Path      string
	FileMode  os.FileMode
	DBOptions *bbolt.Options
}

type bboltDB struct {
	p *properties.Properties

	db *bbolt.DB

	r       *util.RowCodec
	bufPool *util.BufPool
}

func (c bboltCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	opts := getOptions(p)

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		os.RemoveAll(opts.Path)
	}

	db, err := bbolt.Open(opts.Path, opts.FileMode, opts.DBOptions)
	if err != nil {
		return nil, err
	}

	return &bboltDB{
		p:       p,
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func getOptions(p *properties.Properties) bboltOptions {
	path := p.GetString(bboltPath, "/tmp/bbolt")

	opts := bbolt.DefaultOptions
	opts.Timeout = p.GetDuration(bboltTimeout, 0)
	opts.NoGrowSync = p.GetBool(bboltNoGrowSync, false)
	opts.ReadOnly = p.GetBool(bboltReadOnly, false)
	opts.MmapFlags = p.GetInt(bboltMmapFlags, 0)
	opts.InitialMmapSize = p.GetInt(bboltInitialMmapSize, 0)
	opts.FreelistType = bbolt.FreelistType(p.GetString(bboltFreelistType, string(bbolt.FreelistArrayType)))

	return bboltOptions{
		Path:      path,
		FileMode:  0600,
		DBOptions: opts,
	}
}

func (db *bboltDB) Close() error {
	return db.db.Close()
}

func (db *bboltDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *bboltDB) CleanupThread(_ context.Context) {
}

func (db *bboltDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var m map[string][]byte
	err := db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		row := bucket.Get([]byte(key))
		if row == nil {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		var err error
		m, err = db.r.Decode(row, fields)
		return err
	})
	return m, err
}

func (db *bboltDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	err := db.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		cursor := bucket.Cursor()
		key, value := cursor.Seek([]byte(startKey))
		for i := 0; key != nil && i < count; i++ {
			m, err := db.r.Decode(value, fields)
			if err != nil {
				return err
			}

			res[i] = m
			key, value = cursor.Next()
		}

		return nil
	})
	return res, err
}

func (db *bboltDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return fmt.Errorf("table not found: %s", table)
		}

		value := bucket.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("key not found: %s.%s", table, key)
		}

		data, err := db.r.Decode(value, nil)
		if err != nil {
			return err
		}

		for field, value := range values {
			data[field] = value
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, data)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), buf)
	})
	return err
}

func (db *bboltDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	err := db.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			return err
		}

		buf := db.bufPool.Get()
		defer func() {
			db.bufPool.Put(buf)
		}()

		buf, err = db.r.Encode(buf, values)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(key), buf)
	})
	return err
}

func (db *bboltDB) Delete(ctx context.Context, table string, key string) error {
	err := db.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(table))
		if bucket == nil {
			return nil
		}

		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}

		if bucket.Stats().KeyN == 0 {
			_ = tx.DeleteBucket([]byte(table))
		}
		return nil
	})
	return err
}

func init() {
	ycsb.RegisterDBCreator("bbolt", bboltCreator{})
}
