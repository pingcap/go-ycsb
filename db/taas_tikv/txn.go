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

package taas_tikv

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"sync/atomic"

	"log"
	"strconv"
	"unsafe"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"time"
)

//#include ""
import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/go-ycsb/db/taas_proto"
	tikverr "github.com/tikv/client-go/v2/error"
)

const (
	tikvAsyncCommit = "tikv.async_commit"
	tikvOnePC       = "tikv.one_pc"
)

type txnConfig struct {
	asyncCommit bool
	onePC       bool
}

type txnDB struct {
	db      *txnkv.Client
	r       *util.RowCodec
	bufPool *util.BufPool
	cfg     *txnConfig
}

func createTxnDB(p *properties.Properties) (ycsb.DB, error) {
	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")

	fmt.Println("taas_tikv createTxnDB")
	db, err := txnkv.NewClient(strings.Split(pdAddr, ","))
	if err != nil {
		return nil, err
	}

	cfg := txnConfig{
		asyncCommit: p.GetBool(tikvAsyncCommit, true),
		onePC:       p.GetBool(tikvOnePC, true),
	}

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

	fmt.Println("taas_tikv client.go Init OK")

	return &txnDB{
		db: db,
		//db:      nil,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
		cfg:     &cfg,
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

func (db *txnDB) beginTxn() (*transaction.KVTxn, error) {
	txn, err := db.db.Begin()
	if err != nil {
		return nil, err
	}

	txn.SetEnableAsyncCommit(db.cfg.asyncCommit)
	txn.SetEnable1PC(db.cfg.onePC)

	return txn, err
}

func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row, err := tx.Get(ctx, db.getRowKey(table, key))
	if tikverr.IsErrNotFound(err) {
		return nil, nil
	} else if row == nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	return db.r.Decode(row, fields)
}

func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rowValues := make([]map[string][]byte, len(keys))
	for i, key := range keys {
		value, err := tx.Get(ctx, db.getRowKey(table, key))
		if tikverr.IsErrNotFound(err) || value == nil {
			rowValues[i] = nil
		} else {
			rowValues[i], err = db.r.Decode(value, fields)
			if err != nil {
				return nil, err
			}
		}
	}

	return rowValues, nil
}

func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	tx, err := db.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	it, err := tx.Iter(db.getRowKey(table, startKey), nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	rows := make([][]byte, 0, count)
	for i := 0; i < count && it.Valid(); i++ {
		value := append([]byte{}, it.Value()...)
		rows = append(rows, value)
		if err = it.Next(); err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

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
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(buf)
	}()

	buf, err := db.r.Encode(buf, values)
	if err != nil {
		return err
	}

	tx, err := db.beginTxn()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	if err = tx.Set(db.getRowKey(table, key), buf); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, key := range keys {
		rowData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (db *txnDB) Delete(ctx context.Context, table string, key string) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}

	defer tx.Rollback()

	err = tx.Delete(db.getRowKey(table, key))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	tx, err := db.beginTxn()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, key := range keys {
		if err != nil {
			return err
		}
		err = tx.Delete(db.getRowKey(table, key))
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
