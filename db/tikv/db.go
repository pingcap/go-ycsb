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

package tikv

import (
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

const (
	tikvPD = "tikv.pd"
	// raw, txn, or coprocessor
	tikvType = "tikv.type"
)

type tikvCreator struct {
}

func (c tikvCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	tp := p.GetString(tikvType, "raw")
	switch tp {
	case "raw":
		return createRawDB(p)
	case "txn":
		return createTxnDB(p)
	case "coprocessor":
		return createCoprocessorDB(p)
	default:
		return nil, fmt.Errorf("unsupported type %s", tp)
	}
}

func init() {
	ycsb.RegisterDBCreator("tikv", tikvCreator{})
}
