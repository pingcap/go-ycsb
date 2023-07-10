package taas_tikv

import (
	"bytes"
	"compress/gzip"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"github.com/pingcap/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"io/ioutil"
	"log"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

//#include ""
import (
	"context"
	"github.com/golang/protobuf/proto"

	"github.com/pingcap/go-ycsb/db/taas_proto"
)

var TaasServerIp = "172.30.67.185"
var LocalServerIp = "172.30.67.187"
var OpNum = 10

type TaasTxn struct {
	GzipedTransaction []byte
}

var TaasTxnCH = make(chan TaasTxn, 100000)
var UnPackCH = make(chan string, 100000)

var ChanList []chan string
var InitOk uint64 = 0
var atomicCounter uint64 = 0
var SuccessTransactionCounter, FailedTransactionCounter, TotalTransactionCounter uint64 = 0, 0, 0
var SuccessReadCounter, FailedReadCounter, TotalReadCounter uint64 = 0, 0, 0
var SuccessUpdateCounter, FailedUpdateounter, TotalUpdateCounter uint64 = 0, 0, 0
var TotalLatency, TikvReadLatency, TikvTotalLatency uint64 = 0, 0, 0
var latency []uint64

func (db *txnDB) CommitToTaas(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for InitOk == 0 {
		time.Sleep(50)
	}
	//fmt.Println("taas_tikv commit")
	fmt.Println("taas_tikv commit")
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

	var readOpNum, writeOpNum uint64 = 0, 0
	time1 := time.Now()
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, key := range keys {
		if values[i] == nil { //read
			readOpNum++
			rowKey := db.getRowKey(table, key)
			time2 := time.Now()
			rowData, err := tx.Get(ctx, rowKey)
			timeLen2 := time.Now().Sub(time2)
			atomic.AddUint64(&TikvReadLatency, uint64(timeLen2))

			if tikverr.IsErrNotFound(err) {
				return err
			} else if rowData == nil {
				return err
			}
			sendRow := taas_proto.Row{
				OpType: taas_proto.OpType_Read,
				Key:    *(*[]byte)(unsafe.Pointer(&rowKey)),
				Data:   rowData,
				Csn:    0,
			}
			txnSendToTaas.Row = append(txnSendToTaas.Row, &sendRow)
			//fmt.Println("; Read, key : " + string(rowKey) + " Data : " + string(rowData))
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
			//fmt.Print("; Update, key : " + string(rowKey))
			//fmt.Println("; Write, key : " + string(rowKey) + " Data : " + string(rowData))
		}

	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	timeLen := time.Now().Sub(time1)
	atomic.AddUint64(&TikvTotalLatency, uint64(timeLen))
	fmt.Println("; read op : " + strconv.FormatUint(readOpNum, 10) + ", write op : " + strconv.FormatUint(writeOpNum, 10))

	sendMessage := &taas_proto.Message{
		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	}
	var bufferBeforeGzip bytes.Buffer
	sendBuffer, _ := proto.Marshal(sendMessage)
	bufferBeforeGzip.Reset()
	gw := gzip.NewWriter(&bufferBeforeGzip)
	_, err = gw.Write(sendBuffer)
	if err != nil {
		return err
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	GzipedTransaction := bufferBeforeGzip.Bytes()
	GzipedTransaction = GzipedTransaction
	fmt.Println("Send to Taas")
	TaasTxnCH <- TaasTxn{GzipedTransaction}

	result, ok := <-(ChanList[txnId%2048])
	fmt.Println("Receive From Taas")
	t2 := uint64(time.Now().UnixNano() - t1)
	TotalLatency += t2
	//append(latency, t2)
	//result, ok := "Abort", true
	atomic.AddUint64(&TotalReadCounter, uint64(readOpNum))
	atomic.AddUint64(&TotalUpdateCounter, uint64(writeOpNum))
	if ok {
		if result != "Commit" {
			atomic.AddUint64(&FailedReadCounter, uint64(readOpNum))
			atomic.AddUint64(&FailedUpdateounter, uint64(writeOpNum))
			atomic.AddUint64(&FailedTransactionCounter, 1)
			fmt.Println("Commit Failed")
			return errors.New("txn conflict handle failed")
		}
		atomic.AddUint64(&SuccessReadCounter, uint64(readOpNum))
		atomic.AddUint64(&SuccessUpdateCounter, uint64(writeOpNum))
		atomic.AddUint64(&SuccessTransactionCounter, 1)
		fmt.Println("Commit Success")
	} else {
		fmt.Println("txn_bak.go 481")
		log.Fatal(ok)
		return err
	}
	return nil
}

func SendTxnToTaas() {
	socket, _ := zmq.NewSocket(zmq.PUSH)
	err := socket.SetSndbuf(10000000)
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(10000000)
	if err != nil {
		return
	}
	err = socket.SetSndhwm(10000000)
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(10000000)
	if err != nil {
		return
	}
	err = socket.Connect("tcp://" + TaasServerIp + ":5551")
	if err != nil {
		fmt.Println("taas.go 97")
		log.Fatal(err)
	}
	fmt.Println("连接Taas Send " + TaasServerIp)
	for {
		value, ok := <-TaasTxnCH
		if ok {
			_, err := socket.Send(string(value.GzipedTransaction), 0)
			fmt.Println("taas send thread")
			if err != nil {
				return
			}
		} else {
			fmt.Println("taas.go 109")
			log.Fatal(ok)
		}
	}
}

func ListenFromTaas() {
	socket, err := zmq.NewSocket(zmq.PULL)
	err = socket.SetSndbuf(10000000)
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(10000000)
	if err != nil {
		return
	}
	err = socket.SetSndhwm(10000000)
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(10000000)
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
			fmt.Println("taas.go 115")
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
				fmt.Println("taas.go 142")
				log.Fatal(err)
			}
			replyMessage := testMessage.GetReplyTxnResultToClient()
			ChanList[replyMessage.ClientTxnId%2048] <- replyMessage.GetTxnState().String()
		} else {
			fmt.Println("taas.go 148")
			log.Fatal(ok)
		}
	}
}
