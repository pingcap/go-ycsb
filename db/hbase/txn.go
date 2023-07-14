package hbase

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"net"
	"reflect"
	"sync/atomic"

	"log"
	"strconv"
	"unsafe"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"

	"time"
)

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/go-ycsb/db/taas_proto"
)

//const (
//	tikvAsyncCommit = "tikv.async_commit"
//	tikvOnePC       = "tikv.one_pc"
//)

const (
	HOST = "127.0.0.1"
	PORT = "9090"
)

type txnConfig struct {
	asyncCommit bool
	onePC       bool
}

type txnDB struct {
	db      *THBaseServiceClient
	r       *util.RowCodec
	bufPool *util.BufPool
	//cfg     *txnConfig
	//needed by HBase
	transport *thrift.TSocket
}

func createTxnDB(p *properties.Properties) (ycsb.DB, error) {
	//pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
	//
	//fmt.Println("taas_tikv createTxnDB")
	//db, err := txnkv.NewClient(strings.Split(pdAddr, ","))
	//if err != nil {
	//	return nil, err
	//}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transport, err := thrift.NewTSocket(net.JoinHostPort(HOST, PORT))
	if err != nil {
		return nil, err
	}
	db := NewTHBaseServiceClientFactory(transport, protocolFactory)

	//cfg := txnConfig{
	//	asyncCommit: p.GetBool(tikvAsyncCommit, true),
	//	onePC:       p.GetBool(tikvOnePC, true),
	//}

	bufPool := util.NewBufPool()

	TaasServerIp = p.GetString("taasAddress", "")
	LocalServerIp = p.GetString("localServerIp", "")
	OpNum = p.GetInt("opNum", 10)
	for i := 0; i < 2048; i++ {
		ChanList = append(ChanList, make(chan string, 100000))
	}

	go SendTxnToTaas()
	go ListenFromTaas()
	for i := 0; i < 32; i++ {
		go UnPack()
	}
	InitOk = 1

	fmt.Println("hbase client.go Init OK")

	return &txnDB{
		db:        db,
		r:         util.NewRowCodec(p),
		bufPool:   bufPool,
		transport: transport,
	}, nil
}

func (db *txnDB) Close() error {
	//return db.db.Close()
	//seems that HBase
	return db.transport.Close()
}

func (db *txnDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *txnDB) CleanupThread(ctx context.Context) {
}

func (db *txnDB) getRowKey(table string, key string) []byte {
	return util.Slice(fmt.Sprintf("%s:%s", table, key))
}

// seems that HBase don't need this function
func (db *txnDB) beginTxn() (*transaction.KVTxn, error) {
	//txn, err := db.db.Begin()
	//if err != nil {
	//	return nil, err
	//}
	//
	//txn.SetEnableAsyncCommit(db.cfg.asyncCommit)
	//txn.SetEnable1PC(db.cfg.onePC)
	//
	//return txn, err
	return nil, nil
}

func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	//tx, err := db.db.Begin()
	//if err != nil {
	//	return nil, err
	//}
	//defer tx.Rollback()

	tx := db.db
	row, err := tx.Get(ctx, []byte(table), &TGet{Row: []byte(key)})

	if err != nil {
		return nil, err
	}

	columnValues := row.ColumnValues
	res := make(map[string][]byte)
	for _, column := range columnValues {
		c := reflect.ValueOf(column).Elem()
		family := c.Field(0)
		value := c.Field(2)
		res[string(family.Interface().([]uint8))] = value.Interface().([]byte)
	}

	return res, nil
}

func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	//tx, err := db.db.Begin()
	//if err != nil {
	//	return nil, err
	//}
	//defer tx.Rollback()

	tx := db.db

	var tempTGet []*TGet

	rowValues := make([]map[string][]byte, len(keys))
	keyLoc := make(map[string]int)
	for i, key := range keys {
		tempTGet = append(tempTGet, &TGet{
			Row: []byte(key),
		})
		keyLoc[key] = i
	}

	res, err := tx.GetMultiple(ctx, []byte(table), tempTGet)

	if err != nil {
		return nil, err
	}

	for _, columnValues := range res {
		i := keyLoc[string(columnValues.Row)]
		column := columnValues.ColumnValues
		c := reflect.ValueOf(column).Elem()
		family := c.Field(0)
		value := c.Field(2)
		rowValues[i][string(family.Interface().([]uint8))] = value.Interface().([]byte)
	}
	return rowValues, nil
}

// TODO I don't know how to use scanner in HBase
func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	//tx, err := db.db.Begin()
	//if err != nil {
	//	return nil, err
	//}
	//defer tx.Rollback()

	//tx := db.db
	//tx.OpenScanner()
	//
	//it, err := tx.Iter(db.getRowKey(table, startKey), nil)
	//if err != nil {
	//	return nil, err
	//}
	//defer it.Close()
	//
	//rows := make([][]byte, 0, count)
	//for i := 0; i < count && it.Valid(); i++ {
	//	value := append([]byte{}, it.Value()...)
	//	rows = append(rows, value)
	//	if err = it.Next(); err != nil {
	//		return nil, err
	//	}
	//}
	//
	//if err = tx.Commit(ctx); err != nil {
	//	return nil, err
	//}
	//
	//res := make([]map[string][]byte, len(rows))
	//for i, row := range rows {
	//	if row == nil {
	//		res[i] = nil
	//		continue
	//	}
	//
	//	v, err := db.r.Decode(row, fields)
	//	if err != nil {
	//		return nil, err
	//	}
	//	res[i] = v
	//}

	return nil, nil
}

// in HBase, 'update' is equal to 'put', so only implement Insert() is ok
func (db *txnDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	for InitOk == 0 {
		time.Sleep(50)
	}
	txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	atomic.AddUint64(&TotalTransactionCounter, 1)

	rowKey := db.getRowKey(table, key)
	var bufferBeforeGzip bytes.Buffer
	clientIP := LocalServerIp
	txnSendToTaas := taas_proto.Transaction{
		//Row:         {},
		StartEpoch:  0,
		CommitEpoch: 5,
		Csn:         uint64(time.Now().UnixNano()),
		ServerIp:    TaasServerIp,
		ServerId:    0,
		ClientIp:    clientIP,
		ClientTxnId: txnId,
		TxnType:     taas_proto.TxnType_ClientTxn,
		TxnState:    0,
	}
	updateKey := rowKey
	sendRow := taas_proto.Row{
		OpType: taas_proto.OpType_Update,
		Key:    *(*[]byte)(unsafe.Pointer(&updateKey)),
	}
	for field, value := range values {
		idColumn, _ := strconv.ParseUint(string(field[5]), 10, 32)
		updatedColumn := taas_proto.Column{
			Id:    uint32(idColumn),
			Value: value,
		}
		sendRow.Column = append(sendRow.Column, &updatedColumn)
	}
	txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
	sendMessage := &taas_proto.Message{
		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	}
	sendBuffer, _ := proto.Marshal(sendMessage)
	bufferBeforeGzip.Reset()
	gw := gzip.NewWriter(&bufferBeforeGzip)
	_, err := gw.Write(sendBuffer)
	if err != nil {
		return err
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	GzipedTransaction := bufferBeforeGzip.Bytes()
	TaasTxnCH <- TaasTxn{GzipedTransaction}

	result, ok := <-(ChanList[txnId%2048])
	if ok {
		if result != "Commit" {
			atomic.AddUint64(&FailedTransactionCounter, 1)
			return err
		}
		atomic.AddUint64(&SuccessTransactionCounter, 1)
	} else {
		fmt.Println("txn_bak.go 481")
		log.Fatal(ok)
		return err
	}
	return nil
}

func (db *txnDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	atomic.AddUint64(&TotalTransactionCounter, 1)
	for i, key := range keys {
		fmt.Println(string(txnId) + ", i:" + string(i) + ", key:" + key)
	}
	//tx, err := db.beginTxn()
	//if err != nil {
	//	return err
	//}
	//defer tx.Rollback()
	//
	//for i, key := range keys {
	//	// TODO should we check the key exist?
	//	rowData, err := db.r.Encode(nil, values[i])
	//	if err != nil {
	//		return err
	//	}
	//	if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
	//		return err
	//	}
	//}
	//return tx.Commit(ctx)
	return nil
}

func (db *txnDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := db.db
	var cvarr []*TColumnValue
	for k, v := range values {
		cvarr = append(cvarr, &TColumnValue{
			Family:    []byte(k),
			Qualifier: []byte(""),
			Value:     v,
		})
	}
	tempTPut := TPut{Row: []byte(key), ColumnValues: cvarr}
	err := client.Put(ctx, []byte(table), &tempTPut)
	return err
}

func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {

	client := db.db

	var tempTPuts []*TPut
	for i, key := range keys {
		var cvarr []*TColumnValue
		for k, v := range values[i] {
			cvarr = append(cvarr, &TColumnValue{
				Family:    []byte(k),
				Qualifier: []byte(""),
				Value:     v,
			})
		}
		tempTPuts = append(tempTPuts, &TPut{
			Row:          []byte(key),
			ColumnValues: cvarr,
		})
	}

	return client.PutMultiple(ctx, []byte(table), tempTPuts)

}

func (db *txnDB) Delete(ctx context.Context, table string, key string) error {

	client := db.db

	tdelete := TDelete{Row: []byte(key)}
	err := client.DeleteSingle(ctx, []byte(table), &tdelete)
	return err
}

func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {

	client := db.db

	var tDeletes []*TDelete

	for _, key := range keys {
		tDeletes = append(tDeletes, &TDelete{
			Row: []byte(key),
		})
	}

	_, err := client.DeleteMultiple(ctx, []byte(table), tDeletes)
	return err
}
