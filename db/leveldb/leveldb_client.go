package leveldb

import (
	"context"
	"fmt"

	"github.com/icexin/brpc-go"
	bstd "github.com/icexin/brpc-go/protocol/brpc-std"
	"github.com/magiconair/properties"
)

type LeveldbClient struct {
	client LeveldbServiceClient
}

func (c *LeveldbClient) Connect(p *properties.Properties) error {
	endpoint := p.GetString("leveldb.endpoint", "127.0.0.1:8000")
	clientConn, err := brpc.Dial(bstd.ProtocolName, endpoint)
	if err != nil {
		return err
	}
	c.client = NewLeveldbServiceClient(clientConn)
	return nil
}

func (c *LeveldbClient) Put(key []byte, value []byte) error {
	kv_pair := &KVPair{
		Key:   key,
		Value: value,
	}

	request := &LeveldbRequest{
		KvPair: kv_pair,
	}
	response, err := c.client.Put(context.Background(), request)
	if !response.GetSuccess() {
		fmt.Println("Put to leveldb failed")
	}
	return err
}

func (c *LeveldbClient) Get(key []byte) ([]byte, error) {
	kv_pair := &KVPair{
		Key: key,
	}

	request := &LeveldbRequest{
		KvPair: kv_pair,
	}
	response, err := c.client.Get(context.Background(), request)
	if !response.GetSuccess() {
		fmt.Println("Get from leveldb failed")
		return nil, err
	}
	return response.GetValue(), err
}

// delete接口未实现
// func (c *LeveldbClient) Delete(key []byte) error {
// 	kv_pair := &KVPair{
// 		Key: key,
// 	}

// 	request := &LeveldbRequest{
// 		KvPair: kv_pair,
// 	}

// 	response, err := c.client.Execute(context.Background(), request)
// 	if !response.GetSuccess() {
// 		fmt.Println("Delete from leveldb failed")
// 	}
// 	return err
// }
