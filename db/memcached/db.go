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

package memcached

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"strings"
)

// properties
const (
	memcachedHosts        = "memcached.hosts"
	memcachedTimeout      = "memcached.timeout"
	memcachedMaxIdleConns = "memcached.maxIdleConns"
)

type mcCreator struct{}

type mcDB struct {
	p      *properties.Properties
	client *memcache.Client
}

func init() {
	ycsb.RegisterDBCreator("memcached", mcCreator{})
}

func (c mcCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	hosts := p.GetString(memcachedHosts, "localhost:11211")
	timeout := p.GetDuration(memcachedTimeout, memcache.DefaultTimeout)
	maxIdleConns := p.GetInt(memcachedMaxIdleConns, memcache.DefaultMaxIdleConns)

	client := memcache.New(strings.Split(hosts, ",")...)
	client.Timeout = timeout
	client.MaxIdleConns = maxIdleConns

	return &mcDB{
		p:      p,
		client: client,
	}, nil
}

func (db *mcDB) Close() error {
	return db.client.Close()
}

func (db *mcDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *mcDB) CleanupThread(_ context.Context) {}

func (db *mcDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	item, err := db.client.Get(getRowKey(table, key))
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var r map[string][]byte
	err = json.NewDecoder(bytes.NewReader(item.Value)).Decode(&r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (db *mcDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (db *mcDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	item, err := db.client.Get(getRowKey(table, key))
	if errors.Is(err, memcache.ErrCacheMiss) {
		return db.Insert(ctx, table, key, values)
	}

	var m map[string][]byte
	err = json.NewDecoder(bytes.NewReader(item.Value)).Decode(&m)
	for field, value := range values {
		m[field] = value
	}

	data, err := json.Marshal(m)
	if err != nil {
		return err
	}

	item = &memcache.Item{
		Key:   getRowKey(table, key),
		Value: data,
	}
	err = db.client.Set(item)
	if err != nil {
		return err
	}
	return nil
}

func (db *mcDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}
	item := &memcache.Item{
		Key:   getRowKey(table, key),
		Value: data,
	}
	err = db.client.Add(item)
	if err != nil {
		return err
	}
	return nil
}

func (db *mcDB) Delete(ctx context.Context, table string, key string) error {
	return db.client.Delete(getRowKey(table, key))
}

func getRowKey(table, key string) string {
	return fmt.Sprintf("%s:%s", table, key)
}
