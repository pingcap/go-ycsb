package taas_leveldb

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/syndtr/goleveldb/leveldb"
)

type txnConfig struct {
	asyncCommit bool
	onePC       bool
}

type txnDB struct {
	db      *leveldb.DB
	r       *util.RowCodec
	bufPool *util.BufPool
}

var LeveldbConnection []*leveldb.DB
var ClientConnectionNum int = 256

func createTxnDB(p *properties.Properties) (ycsb.DB, error) {
	TaasServerIp = p.GetString("taasServerIp", "")
	// leveldb找到本地leveldb数据库文件夹进行连接，需修改
	dir := p.GetString("leveldb.dir", "/tmp/leveldb")
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, err
	}
	return &txnDB{
		db:      db,
		r:       util.NewRowCodec(p),
		bufPool: util.NewBufPool(),
	}, nil
}

func (db *txnDB) Close() error {
	return db.db.Close()
}

func (db *txnDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *txnDB) CleanupThread(ctx context.Context) {
}

func (db *txnDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	value, err := db.db.Get(db.getRowKey(table, key), nil)
	if err != nil {
		return nil, err
	}
	return db.r.Decode(value, fields)
}

func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := make([]map[string][]byte, len(keys))
	for i, key := range keys {
		value, err := db.db.Get(db.getRowKey(table, key), nil)
		if value == nil {
			rowKeys[i] = nil
		} else {
			rowKeys[i], err = db.r.Decode(value, fields)
			if err != nil {
				return nil, err
			}
		}
	}
	return rowKeys, nil
}

func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	res := make([]map[string][]byte, count)
	it := db.db.NewIterator(nil, nil)
	defer it.Release()

	rowStartKey := db.getRowKey(table, startKey)
	i := 0
	for ok := it.Seek(rowStartKey); ok; ok = it.Next() {
		value, err := db.r.Decode(it.Value(), fields)
		if err != nil {
			return nil, err
		}
		res[i] = value
		i++
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return res, nil
}

// unfinished Update
func (db *txnDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	for InitOk == 0 {
		time.Sleep(50)
	}
	fmt.Println("unsure Update()")
	// original version
	rowKey := db.getRowKey(table, key)
	m, err := db.Read(ctx, table, key, nil)
	fmt.Println(m)
	if err != nil {
		return err
	}
	for field, value := range values {
		m[field] = value
	}
	buf := db.bufPool.Get()
	buf, err = db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	batch.Put(rowKey, buf)
	return db.db.Write(batch, nil)
}

// unfinished batchUpdate
func (db *txnDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	fmt.Println("unsure BatchUpdate()")
	txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	atomic.AddUint64(&TotalTransactionCounter, 1)

	batch := new(leveldb.Batch)
	buf := db.bufPool.Get()
	for i, key := range keys {
		fmt.Println(string(txnId) + ", i:" + string(i) + ", key:" + key)
		m, err := db.Read(ctx, table, key, nil)
		if err != nil {
			return err
		}
		for field, value := range values[i] {
			m[field] = value
		}
		rowKey := db.getRowKey(table, key)
		buf, err = db.r.Encode(buf, values[i])
		if err != nil {
			return err
		}
		batch.Put(rowKey, buf)
	}

	return db.db.Write(batch, nil)
}

func (db *txnDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowKey := db.getRowKey(table, key)
	buf := db.bufPool.Get()
	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}
	batch := new(leveldb.Batch)
	batch.Put(rowKey, buf)
	return db.db.Write(batch, nil)
}

func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	batch := new(leveldb.Batch)
	for i, key := range keys {
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		batch.Put(db.getRowKey(table, key), rowData)
	}
	return db.db.Write(batch, nil)
}

func (db *txnDB) Delete(ctx context.Context, table string, key string) error {
	batch := new(leveldb.Batch)
	rowKey := db.getRowKey(table, key)
	batch.Delete(rowKey)
	return db.db.Write(batch, nil)
}

func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	batch := new(leveldb.Batch)
	for _, key := range keys {
		batch.Delete(db.getRowKey(table, key))
	}
	return db.db.Write(batch, nil)
}
