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
	"strings"
	"sync"

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
