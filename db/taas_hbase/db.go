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

package taas_hbase

import (
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/v2/config"
)

const (
	tikvPD = "tikv.pd"
	// raw, txn, or coprocessor
	tikvType       = "tikv.type"
	tikvConnCount  = "tikv.conncount"
	tikvBatchSize  = "tikv.batchsize"
	tikvAPIVersion = "tikv.apiversion"
)

type taas_hbaseCreator struct {
}

func (c taas_hbaseCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	config.UpdateGlobal(func(c *config.Config) {
		c.TiKVClient.GrpcConnectionCount = p.GetUint(tikvConnCount, 128)
		c.TiKVClient.MaxBatchSize = p.GetUint(tikvBatchSize, 128)
	})

	tp := p.GetString(tikvType, "txn")
	fmt.Println("=====================  Taas - HBase  ============================")
	switch tp {
	case "raw":
		return nil, nil
		///todo not implement yet
		//return createRawDB(p)
	case "txn":
		return createTxnDB(p)
	default:
		return nil, fmt.Errorf("unsupported type %s", tp)
	}
}

func init() {
	ycsb.RegisterDBCreator("taas_hbase", taas_hbaseCreator{})
}
