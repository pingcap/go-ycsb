package taas_leveldb

import (
	"context"

	brpc_go "github.com/icexin/brpc-go"
)

type LeveldbClient struct {
	conn brpc_go.ClientConn
}

func (c *LeveldbClient) Get(key []byte) (value []byte, err error) {
	getClient := NewKvDBGetServiceClient(c.conn)

	kv_pair := &KvDBData{
		Key: string(key),
	}

	request := &KvDBRequest{
		Data: []*KvDBData{kv_pair},
	}
	response, err := getClient.Get(context.Background(), request)
	if err != nil {
		return nil, err
	}
	return []byte(response.Data[0].Value), nil
}

func (c *LeveldbClient) Put(key, value []byte) error {
	putClient := NewKvDBPutServiceClient(c.conn)
	kv_pair := &KvDBData{
		Key:   string(key),
		Value: string(value),
	}

	request := &KvDBRequest{
		Data: []*KvDBData{kv_pair},
	}
	_, err := putClient.Put(context.Background(), request)
	return err
}
