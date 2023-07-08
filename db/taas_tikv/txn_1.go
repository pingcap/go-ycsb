//// Copyright 2018 PingCAP, Inc.
////
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
////
////     http://www.apache.org/licenses/LICENSE-2.0
////
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// See the License for the specific language governing permissions and
//// limitations under the License.
//
package taas_tikv

//
//import (
//	"bytes"
//	"compress/gzip"
//	"fmt"
//	"github.com/Mister-Star/go-ycsb/db/taas_proto"
//	"github.com/pingcap/errors"
//	"io/ioutil"
//	"sync/atomic"
//
//	"log"
//	"strconv"
//	"unsafe"
//
//	"github.com/magiconair/properties"
//	"github.com/pingcap/go-ycsb/pkg/util"
//	"github.com/pingcap/go-ycsb/pkg/ycsb"
//	"github.com/tikv/client-go/v2/txnkv"
//	"github.com/tikv/client-go/v2/txnkv/transaction"
//	"net"
//	"strings"
//	"time"
//)
//
////#include ""
//import (
//	"context"
//	"github.com/golang/protobuf/proto"
//
//	zmq "github.com/pebbe/zmq4"
//	tikverr "github.com/tikv/client-go/v2/error"
//)
//
//const (
//	tikvAsyncCommit = "tikv.async_commit"
//	tikvOnePC       = "tikv.one_pc"
//)
//
//type txnConfig struct {
//	asyncCommit bool
//	onePC       bool
//}
//
//type txnDB struct {
//	db      *txnkv.Client
//	r       *util.RowCodec
//	bufPool *util.BufPool
//	cfg     *txnConfig
//}
//
//const TaasServerIp = "172.30.67.201"
//
//type TaasTxn struct {
//	GzipedTransaction []byte
//}
//
//var TaasTxnCH = make(chan TaasTxn, 100000)
//var UnPackCH = make(chan string, 100000)
//
//var ChanList []chan string
//var init_ok uint64 = 0
//var atomic_counter uint64 = 0
//
//func SendTxnToTaas() {
//	fmt.Println("连接taas Send")
//
//	socket, _ := zmq.NewSocket(zmq.PUSH)
//	socket.SetSndbuf(1000000000000000)
//	socket.SetRcvbuf(1000000000000000)
//	socket.SetSndhwm(1000000000000000)
//	socket.SetRcvhwm(1000000000000000)
//	socket.Connect("tcp://" + TaasServerIp + ":5551")
//
//	// dealer, _ := goczmq.NewPush("tcp://" + TaasServerIp + ":5551")
//
//	fmt.Println("连接成功 Send" + TaasServerIp)
//	for {
//		value, ok := <-TaasTxnCH
//		if ok {
//			// err := dealer.SendFrame(value.GzipedTransaction, 0)
//			socket.Send(string(value.GzipedTransaction), 0)
//			// if err != nil {
//			// 	fmt.Println("txn.go 93")
//			// 	log.Fatal(err)
//			// 	break
//			// }
//		} else {
//			fmt.Println("txn.go 98")
//			log.Fatal(ok)
//			break
//		}
//	}
//}
//
//func ListenFromTaas() {
//	fmt.Println("连接taas Listen")
//	// router, err := goczmq.NewPull("tcp://*:5552")
//	socket, err := zmq.NewSocket(zmq.PULL)
//	socket.SetSndbuf(1000000000000000)
//	socket.SetRcvbuf(1000000000000000)
//	socket.SetSndhwm(1000000000000000)
//	socket.SetRcvhwm(1000000000000000)
//	socket.Bind("tcp://*:5552")
//	fmt.Println("连接成功 Listen")
//	if err != nil {
//		log.Fatal(err)
//	}
//	for {
//		// taas_reply, err := router.RecvMessage()
//		taas_reply, err := socket.Recv(0)
//		if err != nil {
//			fmt.Println("txn.go 115")
//			log.Fatal(err)
//		}
//		UnPackCH <- taas_reply
//		// UnGZipedReply := UGZipBytes(taas_reply[0])
//		// testMessage := &Message{}
//		// err = proto.Unmarshal(UnGZipedReply, testMessage)
//		// if err != nil {
//		// 	fmt.Println("txn.go 118")
//		// 	log.Fatal(err)
//		// }
//		// //fmt.Println(testMessage)
//		// replyMessage := testMessage.GetReplyTxnResultToClient()
//		// //fmt.Println("txn.go 123 " + string(replyMessage.ClientTxnId))
//		// //fmt.Println(replyMessage.ClientTxnId % 2048)
//		// (ChanList[replyMessage.ClientTxnId%2048]) <- string(replyMessage.GetTxnState().String())
//	}
//}
//
//func UnPack() {
//	for {
//		taas_reply, ok := <-UnPackCH
//		if ok {
//			UnGZipedReply := UGZipBytes([]byte(taas_reply))
//			testMessage := &taas_proto.Message{}
//			err := proto.Unmarshal(UnGZipedReply, testMessage)
//			if err != nil {
//				fmt.Println("txn.go 142")
//				log.Fatal(err)
//			}
//			replyMessage := testMessage.GetReplyTxnResultToClient()
//			(ChanList[replyMessage.ClientTxnId%2048]) <- string(replyMessage.GetTxnState().String())
//		} else {
//			fmt.Println("txn.go 148")
//			log.Fatal(ok)
//			break
//		}
//	}
//}
//
//func createTxnDB(p *properties.Properties) (ycsb.DB, error) {
//	pdAddr := p.GetString(tikvPD, "127.0.0.1:2379")
//	db, err := txnkv.NewClient(strings.Split(pdAddr, ","))
//	if err != nil {
//		return nil, err
//	}
//
//	for i := 0; i < 2048; i++ {
//		ChanList = append(ChanList, make(chan string, 100000))
//	}
//
//	init_ok = 1
//	go SendTxnToTaas()
//	go ListenFromTaas()
//	for i := 0; i < 4; i++ {
//		go UnPack()
//	}
//	time.Sleep(5)
//
//	cfg := txnConfig{
//		asyncCommit: p.GetBool(tikvAsyncCommit, true),
//		onePC:       p.GetBool(tikvOnePC, true),
//	}
//
//	bufPool := util.NewBufPool()
//
//	return &txnDB{
//		db:      db,
//		r:       util.NewRowCodec(p),
//		bufPool: bufPool,
//		cfg:     &cfg,
//	}, nil
//}
//
//func (db *txnDB) Close() error {
//	return db.db.Close()
//}
//
//func (db *txnDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
//	return ctx
//}
//
//func (db *txnDB) CleanupThread(ctx context.Context) {
//}
//
//func (db *txnDB) getRowKey(table string, key string) []byte {
//	return util.Slice(fmt.Sprintf("%s:%s", table, key))
//}
//
//func (db *txnDB) beginTxn() (*transaction.KVTxn, error) {
//	txn, err := db.db.Begin()
//	if err != nil {
//		return nil, err
//	}
//
//	txn.SetEnableAsyncCommit(db.cfg.asyncCommit)
//	txn.SetEnable1PC(db.cfg.onePC)
//
//	return txn, err
//}
//
//func (db *txnDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
//	tx, err := db.db.Begin()
//	if err != nil {
//		return nil, err
//	}
//	defer tx.Rollback()
//
//	row, err := tx.Get(ctx, db.getRowKey(table, key))
//	if tikverr.IsErrNotFound(err) {
//		return nil, nil
//	} else if row == nil {
//		return nil, err
//	}
//
//	if err = tx.Commit(ctx); err != nil {
//		return nil, err
//	}
//
//	return db.r.Decode(row, fields)
//}
//
//func (db *txnDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
//	tx, err := db.db.Begin()
//	if err != nil {
//		return nil, err
//	}
//	defer tx.Rollback()
//
//	rowValues := make([]map[string][]byte, len(keys))
//	for i, key := range keys {
//		value, err := tx.Get(ctx, db.getRowKey(table, key))
//		if tikverr.IsErrNotFound(err) || value == nil {
//			rowValues[i] = nil
//		} else {
//			rowValues[i], err = db.r.Decode(value, fields)
//			if err != nil {
//				return nil, err
//			}
//		}
//	}
//
//	return rowValues, nil
//}
//
//func (db *txnDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
//	tx, err := db.db.Begin()
//	if err != nil {
//		return nil, err
//	}
//	defer tx.Rollback()
//
//	it, err := tx.Iter(db.getRowKey(table, startKey), nil)
//	if err != nil {
//		return nil, err
//	}
//	defer it.Close()
//
//	rows := make([][]byte, 0, count)
//	for i := 0; i < count && it.Valid(); i++ {
//		value := append([]byte{}, it.Value()...)
//		rows = append(rows, value)
//		if err = it.Next(); err != nil {
//			return nil, err
//		}
//	}
//
//	if err = tx.Commit(ctx); err != nil {
//		return nil, err
//	}
//
//	res := make([]map[string][]byte, len(rows))
//	for i, row := range rows {
//		if row == nil {
//			res[i] = nil
//			continue
//		}
//
//		v, err := db.r.Decode(row, fields)
//		if err != nil {
//			return nil, err
//		}
//		res[i] = v
//	}
//
//	return res, nil
//}
//
//// 获取本机当前可用端口号
//func GetFreePort() (int, error) {
//	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
//	if err != nil {
//		return 0, err
//	}
//
//	l, err := net.ListenTCP("tcp", addr)
//	if err != nil {
//		return 0, err
//	}
//	defer l.Close()
//	return l.Addr().(*net.TCPAddr).Port, nil
//}
//
//// 获取ip
//func externalIP() (net.IP, error) {
//	ifaces, err := net.Interfaces()
//	if err != nil {
//		return nil, err
//	}
//	for _, iface := range ifaces {
//		if iface.Flags&net.FlagUp == 0 {
//			continue // interface down
//		}
//		if iface.Flags&net.FlagLoopback != 0 {
//			continue // loopback interface
//		}
//		addrs, err := iface.Addrs()
//		if err != nil {
//			return nil, err
//		}
//		for _, addr := range addrs {
//			ip := getIpFromAddr(addr)
//			if ip == nil {
//				continue
//			}
//			return ip, nil
//		}
//	}
//	return nil, errors.New("connected to the network?")
//}
//
//// 获取本机IP地址
//func getIpFromAddr(addr net.Addr) net.IP {
//	var ip net.IP
//	switch v := addr.(type) {
//	case *net.IPNet:
//		ip = v.IP
//	case *net.IPAddr:
//		ip = v.IP
//	}
//	if ip == nil || ip.IsLoopback() {
//		return nil
//	}
//	ip = ip.To4()
//	if ip == nil {
//		return nil // not an ipv4 address
//	}
//
//	return ip
//}
//
//// 将远端发过来的消息进行解压缩
//func UGZipBytes(in []byte) []byte {
//	reader, err := gzip.NewReader(bytes.NewReader(in))
//	if err != nil {
//		var out []byte
//		return out
//	}
//	defer reader.Close()
//	out, _ := ioutil.ReadAll(reader)
//	return out
//
//}
//
//// 修改代码的主体部分
//func (db *txnDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
//	for init_ok == 0 {
//		time.Sleep(50)
//	}
//	chan_id := atomic.AddUint64(&atomic_counter, 1) // return new value
//	rowKey := db.getRowKey(table, key)
//	var bufferBeforeGzip bytes.Buffer
//	ip, err := externalIP()
//	if err != nil {
//		fmt.Println(err)
//	}
//	clientIP := ip.String()
//	txnSendToTaas := taas_proto.Transaction{
//		//Row:         {},
//		StartEpoch:  0,
//		CommitEpoch: 5,
//		Csn:         uint64(time.Now().UnixNano()),
//		ServerIp:    TaasServerIp,
//		ServerId:    0,
//		ClientIp:    clientIP,
//		ClientTxnId: chan_id,
//		TxnType:     taas_proto.TxnType_ClientTxn,
//		TxnState:    0,
//	}
//	updateKey := rowKey
//	sendRow := taas_proto.Row{
//		OpType: taas_proto.OpType_Update,
//		Key:    *(*[]byte)(unsafe.Pointer(&updateKey)),
//	}
//	for field, value := range values {
//		idColumn, _ := strconv.ParseUint(string(field[5]), 10, 32)
//		updatedColumn := taas_proto.Column{
//			Id:    uint32(idColumn),
//			Value: value,
//		}
//		sendRow.Column = append(sendRow.Column, &updatedColumn)
//	}
//	txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
//	sendMessage := &taas_proto.Message{
//		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
//	}
//	sendBuffer, _ := proto.Marshal(sendMessage)
//	bufferBeforeGzip.Reset()
//	gw := gzip.NewWriter(&bufferBeforeGzip)
//	gw.Write(sendBuffer)
//	gw.Close()
//	GzipedTransaction := bufferBeforeGzip.Bytes()
//	TaasTxnCH <- TaasTxn{GzipedTransaction}
//
//	result, ok := <-(ChanList[chan_id%2048])
//	if ok {
//		if result != "Commit" {
//			return err
//		}
//	} else {
//		fmt.Println("txn.go 481")
//		log.Fatal(ok)
//		return err
//	}
//	return nil
//}
//
//func (db *txnDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
//	tx, err := db.beginTxn()
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	for i, key := range keys {
//		// TODO should we check the key exist?
//		rowData, err := db.r.Encode(nil, values[i])
//		if err != nil {
//			return err
//		}
//		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
//			return err
//		}
//	}
//	return tx.Commit(ctx)
//}
//
//func (db *txnDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
//	// Simulate TiDB data
//	buf := db.bufPool.Get()
//	defer func() {
//		db.bufPool.Put(buf)
//	}()
//
//	buf, err := db.r.Encode(buf, values)
//	if err != nil {
//		return err
//	}
//
//	tx, err := db.beginTxn()
//	if err != nil {
//		return err
//	}
//
//	defer tx.Rollback()
//
//	if err = tx.Set(db.getRowKey(table, key), buf); err != nil {
//		return err
//	}
//
//	return tx.Commit(ctx)
//}
//
//func (db *txnDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
//	tx, err := db.beginTxn()
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	for i, key := range keys {
//		rowData, err := db.r.Encode(nil, values[i])
//		if err != nil {
//			return err
//		}
//		if err = tx.Set(db.getRowKey(table, key), rowData); err != nil {
//			return err
//		}
//	}
//	return tx.Commit(ctx)
//}
//
//func (db *txnDB) Delete(ctx context.Context, table string, key string) error {
//	tx, err := db.beginTxn()
//	if err != nil {
//		return err
//	}
//
//	defer tx.Rollback()
//
//	err = tx.Delete(db.getRowKey(table, key))
//	if err != nil {
//		return err
//	}
//
//	return tx.Commit(ctx)
//}
//
//func (db *txnDB) BatchDelete(ctx context.Context, table string, keys []string) error {
//	tx, err := db.beginTxn()
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	for _, key := range keys {
//		if err != nil {
//			return err
//		}
//		err = tx.Delete(db.getRowKey(table, key))
//		if err != nil {
//			return err
//		}
//	}
//	return tx.Commit(ctx)
//}
