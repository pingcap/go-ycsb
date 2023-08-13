package taas_leveldb

import (
	"context"

	brpc_go "github.com/icexin/brpc-go"
	"github.com/pingcap/go-ycsb/db/taas_proto"
	"github.com/pingcap/go-ycsb/pkg/util"
)

type LeveldbClient struct {
	conn brpc_go.ClientConn
}

func (c *LeveldbClient) Get(key []byte) (value []byte, err error) {
	getClient := taas_proto.NewKvDBGetServiceClient(c.conn)

	kv_pair := &taas_proto.KvDBData{
		Key: util.String(key),
	}

	request := &taas_proto.KvDBRequest{
		Data: []*taas_proto.KvDBData{kv_pair},
	}
	response, err := getClient.Get(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return []byte(response.Data[0].Value), nil
}

func (c *LeveldbClient) Put(key, value []byte) error {
	// value 无法转化为有效的UTF-8 string类型，proto中都是用string
	putClient := taas_proto.NewKvDBPutServiceClient(c.conn)
	kv_pair := &taas_proto.KvDBData{
		Key:   util.String(key),
		Value: util.String(value),
	}

	request := &taas_proto.KvDBRequest{
		Data: []*taas_proto.KvDBData{kv_pair},
	}
	_, err := putClient.Put(context.Background(), request)
	return err
}
