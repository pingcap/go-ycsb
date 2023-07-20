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
	fmt.Println("not implement yet")
	// txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	// atomic.AddUint64(&TotalTransactionCounter, 1)

	// var bufferBeforeGzip bytes.Buffer
	// clientIP := LocalServerIp
	// txnSendToTaas := taas_proto.Transaction{ // 存储发送给taas的事务数据
	// 	//Row:         {},
	// 	StartEpoch:  0,
	// 	CommitEpoch: 5,
	// 	Csn:         uint64(time.Now().UnixNano()),
	// 	ServerIp:    TaasServerIp,
	// 	ServerId:    0,
	// 	ClientIp:    clientIP,
	// 	ClientTxnId: txnId,
	// 	TxnType:     taas_proto.TxnType_ClientTxn,
	// 	TxnState:    0,
	// }
	// updateKey := rowKey        // 获取updateKey
	// sendRow := taas_proto.Row{ // 存储发送给Taas的行数据
	// 	OpType: taas_proto.OpType_Update,
	// 	Key:    *(*[]byte)(unsafe.Pointer(&updateKey)),
	// }
	// for field, value := range values { // 遍历values
	// 	idColumn, _ := strconv.ParseUint(string(field[5]), 10, 32) // 获取idColumn
	// 	updatedColumn := taas_proto.Column{                        // 存储更新后的列数据
	// 		Id:    uint32(idColumn),
	// 		Value: value,
	// 	}
	// 	sendRow.Column = append(sendRow.Column, &updatedColumn) // 添加到sendRow.Column中
	// }
	// sendMessage := &taas_proto.Message{ //存储发送给Taas的消息数据
	// 	Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	// }
	// sendBuffer, _ := proto.Marshal(sendMessage) // 将sendMessage序列化为字节数组sendBuffer
	// bufferBeforeGzip.Reset()                    // 重置bufferBeforeGzip
	// gw := gzip.NewWriter(&bufferBeforeGzip)     // 用于压缩数据
	// _, err := gw.Write(sendBuffer)              // 将sendBuffer写入gw
	// if err != nil {
	// 	return err
	// }
	// err = gw.Close()
	// if err != nil {
	// 	return err
	// }
	// GzipedTransaction := bufferBeforeGzip.Bytes()
	// TaasTxnCH <- TaasTxn{GzipedTransaction}

	// result, ok := <-(ChanList[txnId%2048])
	// if ok {
	// 	if result != "Commit" {
	// 		atomic.AddUint64(&FailedTransactionCounter, 1)
	// 		return err
	// 	}
	// 	atomic.AddUint64(&SuccessTransactionCounter, 1)
	// } else {
	// 	fmt.Println("txn_bak.go 481")
	// 	log.Fatal(ok)
	// 	return err
	// }
	// return nil

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
	fmt.Println("not implement yet")
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
