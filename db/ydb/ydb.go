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
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

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

func init() {
	ycsb.RegisterDBCreator("ydb", creator{})
}

type creator struct{}

var (
	_ ycsb.DBCreator = (*creator)(nil)
)

func (c creator) Create(p *properties.Properties) (_ ycsb.DB, err error) {
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			panic(err)
		}
	}()

	dsn := p.GetString(ydbDSN, ydbDSNDefault)

	ctx := context.Background()

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))
	driverType := p.GetString(ydbDriverType, ydbDriverTypeDefault)
	driversCount := p.GetInt(ydbDriversCount, ydbDriversCountDefault)

	d := &driver{
		p:           p,
		verbose:     p.GetBool(prop.Verbose, prop.VerboseDefault),
		forceUpsert: p.GetBool(ydbForceUpsert, ydbForceUpsertDefault),
		cores:       make([]driverCore, driversCount),
	}

	for i := range d.cores {
		var core driverCore
		switch driverType {
		case ydbDriverTypeNative:
			core, err = openNative(ctx, dsn, threadCount/driversCount+1)
		case ydbDriverTypeSql:
			core, err = openSql(ctx, dsn, threadCount/driversCount+1)
		default:
			return nil, fmt.Errorf("unknown driver type '%s'. allowed: [%s, %s]",
				driverType, ydbDriverTypeNative, ydbDriverTypeSql,
			)
		}
		if err != nil {
			return nil, err
		}
		d.cores[i] = core
	}

	return d, d.createTable()
}
