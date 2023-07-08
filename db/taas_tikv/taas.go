package taas_tikv

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"sync/atomic"
	"time"
	"unsafe"
)

//#include ""
import (
	"context"
	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/pingcap/go-ycsb/db/taas_proto"
)

const TaasServerIp = "172.30.67.201"
const LocalServerIp = "172.30.67.201"
const OpNum = 10

type TaasTxn struct {
	GzipedTransaction []byte
}

var TaasTxnCH = make(chan TaasTxn, 100000)
var UnPackCH = make(chan string, 100000)

var ChanList []chan string
var initOk uint64 = 0
var atomicCounter uint64 = 0
var SuccessTransactionCounter, FailedTransactionCounter, TotalTransactionCounter uint64 = 0, 0, 0
var SuccessReadCounter, FailedReadounter, TotalReadCounter uint64 = 0, 0, 0
var SuccessUpdateCounter, FailedUpdateounter, TotalUpdateCounter uint64 = 0, 0, 0
var TotalLatency uint64 = 0
var latency []uint64

func (db *txnDB) CommitToTaas(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for initOk == 0 {
		time.Sleep(50)
	}
	t1 := time.Now().UnixNano()
	txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	atomic.AddUint64(&TotalTransactionCounter, 1)
	txnSendToTaas := taas_proto.Transaction{
		StartEpoch:  0,
		CommitEpoch: 5,
		Csn:         uint64(time.Now().UnixNano()),
		ServerIp:    TaasServerIp,
		ServerId:    0,
		ClientIp:    LocalServerIp,
		ClientTxnId: txnId,
		TxnType:     taas_proto.TxnType_ClientTxn,
		TxnState:    0,
	}

	readOpNum, writeOpNum := 0, 0
	for i, key := range keys {
		if values[i] == nil { //read
			readOpNum++
			rowKey := db.getRowKey(table, key)
			//tx, err := db.db.Begin()
			//if err != nil {
			//	return err
			//}
			//defer tx.Rollback()
			//
			//rowData, err := tx.Get(ctx, rowKey)
			//if tikverr.IsErrNotFound(err) {
			//	return err
			//} else if rowData == nil {
			//	return err
			//}
			//
			//if err = tx.Commit(ctx); err != nil {
			//	return err
			//}
			sendRow := taas_proto.Row{
				OpType: taas_proto.OpType_Update,
				Key:    *(*[]byte)(unsafe.Pointer(&rowKey)),
				Data:   nil,
				Csn:    0,
			}
			txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
		} else {
			writeOpNum++
			rowKey := db.getRowKey(table, key)
			rowData, err := db.r.Encode(nil, values[i])
			if err != nil {
				return err
			}
			sendRow := taas_proto.Row{
				OpType: taas_proto.OpType_Update,
				Key:    *(*[]byte)(unsafe.Pointer(&rowKey)),
				Data:   []byte(rowData),
			}
			txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
		}

	}

	sendMessage := &taas_proto.Message{
		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	}
	var bufferBeforeGzip bytes.Buffer
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
	GzipedTransaction = GzipedTransaction
	//TaasTxnCH <- TaasTxn{GzipedTransaction}
	//
	//result, ok := <-(ChanList[txnId%2048])

	t2 := uint64(time.Now().UnixNano() - t1)
	TotalLatency += t2
	//append(latency, t2)
	result, ok := "Abort", true
	atomic.AddUint64(&TotalReadCounter, uint64(readOpNum))
	atomic.AddUint64(&TotalUpdateCounter, uint64(writeOpNum))
	if ok {
		if result != "Commit" {
			atomic.AddUint64(&FailedReadounter, uint64(readOpNum))
			atomic.AddUint64(&FailedUpdateounter, uint64(writeOpNum))
			atomic.AddUint64(&FailedTransactionCounter, 1)
			return err
		}
		atomic.AddUint64(&SuccessReadCounter, uint64(readOpNum))
		atomic.AddUint64(&SuccessUpdateCounter, uint64(writeOpNum))
		atomic.AddUint64(&SuccessTransactionCounter, 1)
	} else {
		fmt.Println("txn_bak.go 481")
		log.Fatal(ok)
		return err
	}
	return nil
}

func SendTxnToTaas() {
	socket, _ := zmq.NewSocket(zmq.PUSH)
	err := socket.SetSndbuf(1000000000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(1000000000000000)
	if err != nil {
		return
	}
	err = socket.SetSndhwm(1000000000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(1000000000000000)
	if err != nil {
		return
	}
	err = socket.Connect("tcp://" + TaasServerIp + ":5551")
	if err != nil {
		fmt.Println("txn.go 97")
		log.Fatal(err)
	}
	fmt.Println("连接Taas Send" + TaasServerIp)
	for {
		value, ok := <-TaasTxnCH
		if ok {
			_, err := socket.Send(string(value.GzipedTransaction), 0)
			if err != nil {
				return
			}
		} else {
			fmt.Println("txn.go 109")
			log.Fatal(ok)
		}
	}
}

func ListenFromTaas() {
	socket, err := zmq.NewSocket(zmq.PULL)
	err = socket.SetSndbuf(1000000000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(1000000000000000)
	if err != nil {
		return
	}
	err = socket.SetSndhwm(1000000000000000)
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(1000000000000000)
	if err != nil {
		return
	}
	err = socket.Bind("tcp://*:5552")
	fmt.Println("连接Taas Listen")
	if err != nil {
		log.Fatal(err)
	}
	for {
		taasReply, err := socket.Recv(0)
		if err != nil {
			fmt.Println("txn_bak.go 115")
			log.Fatal(err)
		}
		UnPackCH <- taasReply
	}
}

func UGZipBytes(in []byte) []byte {
	reader, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		var out []byte
		return out
	}
	defer reader.Close()
	out, _ := ioutil.ReadAll(reader)
	return out

}

func UnPack() {
	for {
		taasReply, ok := <-UnPackCH
		if ok {
			UnGZipedReply := UGZipBytes([]byte(taasReply))
			testMessage := &taas_proto.Message{}
			err := proto.Unmarshal(UnGZipedReply, testMessage)
			if err != nil {
				fmt.Println("txn_bak.go 142")
				log.Fatal(err)
			}
			replyMessage := testMessage.GetReplyTxnResultToClient()
			ChanList[replyMessage.ClientTxnId%2048] <- string(replyMessage.GetTxnState().String())
		} else {
			fmt.Println("txn_bak.go 148")
			log.Fatal(ok)
		}
	}
}
