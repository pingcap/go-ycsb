package taas_leveldb

import (
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/v2/config"
)

const (
	tikvBatchSize  = "tikv.batchsize"
	tikvAPIVersion = "tikv.apiversion"
	tikvPD         = "tikv.pd"
	// raw, txn, or coprocessor
	tikvType      = "tikv.type"
	tikvConnCount = "tikv.conncount"
)

type taas_leveldbCreator struct {
}

func (c taas_leveldbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	config.UpdateGlobal(func(c *config.Config) {
		c.TiKVClient.GrpcConnectionCount = p.GetUint(tikvConnCount, 128)
		c.TiKVClient.MaxBatchSize = p.GetUint(tikvBatchSize, 128)
	})

	tp := p.GetString(tikvType, "txn")
	fmt.Println("=====================  Taas - leveldb  ============================")
	switch tp {
	case "raw":
		return nil, nil
		///todo not implement yet
		///return createRawDB(p)
	case "txn":
		return createTxnDB(p)
	default:
		return nil, fmt.Errorf("unsupported type %s", tp)
	}
}

func init() {
	ycsb.RegisterDBCreator("taas_leveldb", taas_leveldbCreator{})
}
