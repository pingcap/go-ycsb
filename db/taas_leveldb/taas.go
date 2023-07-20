package taas_leveldb

//#include ""
import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
	"github.com/pingcap/go-ycsb/db/taas_proto"
)

var TaasServerIp = "127.0.0.1"
var LocalServerIp = "127.0.0.1"
var HbaseServerIp = "127.0.0.1"
var OpNum = 10

type TaasTxn struct { // 用于存储压缩后的事务数据
	GzipedTransaction []byte
}

var TaasTxnCH = make(chan TaasTxn, 100000) // 创建通道，存储txn数据
var UnPackCH = make(chan string, 100000)   // 创建通道，存储解压后的数据

var ChanList []chan string
var InitOk uint64 = 0
var atomicCounter uint64 = 0

var SuccessTransactionCounter, FailedTransactionCounter, TotalTransactionCounter uint64 = 0, 0, 0 // 成功事务、失败事务、总事务
var SuccessReadCounter, FailedReadCounter, TotalReadCounter uint64 = 0, 0, 0                      // 成功读取、失败读取、总读取
var SuccessUpdateCounter, FailedUpdateounter, TotalUpdateCounter uint64 = 0, 0, 0                 // 成功更新、失败更新、总更新
var TotalLatency, TikvReadLatency, TikvTotalLatency uint64 = 0, 0, 0                              // 总延迟、读延迟
var latency []uint64                                                                              // 存储延迟数据

func (db *txnDB) CommitToTaas(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for InitOk == 0 {
		time.Sleep(50)
	}

	t1 := time.Now().UnixNano()
	txnId := atomic.AddUint64(&atomicCounter, 1) // return new value
	atomic.AddUint64(&TotalTransactionCounter, 1)
	txnSendToTaas := taas_proto.Transaction{ // 储发送给Taas的事务数据
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
	for i, key := range keys {
		if values[i] == nil { // 如果values[i]为nil，则表示读取操作
			readOpNum++
			rowKey := db.getRowKey(table, key)
			time2 := time.Now()
			rowData, err := db.db.Get(rowKey, nil)
			timeLen2 := time.Now().Sub(time2)
			atomic.AddUint64(&TikvReadLatency, uint64(timeLen2))

			if err != nil {
				return err
			} else if rowData == nil {
				return errors.New("txn read failed")
			}
			sendRow := taas_proto.Row{ // 用于存储发送给Taas的行数据
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

	timeLen := time.Now().Sub(time1)
	atomic.AddUint64(&TikvTotalLatency, uint64(timeLen))
	//fmt.Println("; read op : " + strconv.FormatUint(readOpNum, 10) + ", write op : " + strconv.FormatUint(writeOpNum, 10))

	sendMessage := &taas_proto.Message{ // 存储发送给Taas的消息数据
		Type: &taas_proto.Message_Txn{Txn: &txnSendToTaas},
	}
	var bufferBeforeGzip bytes.Buffer           // 存储压缩前的数据
	sendBuffer, _ := proto.Marshal(sendMessage) // 序列化
	bufferBeforeGzip.Reset()
	gw := gzip.NewWriter(&bufferBeforeGzip) // 用于压缩数据
	_, err := gw.Write(sendBuffer)
	if err != nil {
		return err
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	GzipedTransaction := bufferBeforeGzip.Bytes() // 获取压缩后的数据
	// GzipedTransaction = GzipedTransaction
	//fmt.Println("Send to Taas")
	TaasTxnCH <- TaasTxn{GzipedTransaction} // 发送压缩后的数据

	result, ok := <-(ChanList[txnId%2048])
	//fmt.Println("Receive From Taas")
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
			//fmt.Println("Commit Failed")
			return errors.New("txn conflict handle failed")
		}
		atomic.AddUint64(&SuccessReadCounter, uint64(readOpNum))
		atomic.AddUint64(&SuccessUpdateCounter, uint64(writeOpNum))
		atomic.AddUint64(&SuccessTransactionCounter, 1)
		//fmt.Println("Commit Success")
	} else {
		fmt.Println("txn_bak.go 481")
		log.Fatal(ok)
		return err
	}
	return nil
}

func SendTxnToTaas() {
	socket, _ := zmq.NewSocket(zmq.PUSH)
	err := socket.SetSndbuf(10000000) // 设置socket的发送缓冲区大小
	if err != nil {
		return
	}
	err = socket.SetRcvbuf(10000000) // 设置socket的接收缓冲区大小
	if err != nil {
		return
	}
	err = socket.SetSndhwm(10000000) // 设置发送高水位标记
	if err != nil {
		return
	}
	err = socket.SetRcvhwm(10000000) // 设置接收高水位标记
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
		value, ok := <-TaasTxnCH // 从通道中获取value和状态
		if ok {
			_, err := socket.Send(string(value.GzipedTransaction), 0) // 发送value
			//fmt.Println("taas send thread")
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
		taasReply, err := socket.Recv(0) // 从socket中接收数据taasReply和状态err
		if err != nil {
			fmt.Println("taas.go 115")
			log.Fatal(err)
		}
		UnPackCH <- taasReply // 将taasReply解压后的数据发送到通道中
	}
}

func UGZipBytes(in []byte) []byte {
	reader, err := gzip.NewReader(bytes.NewReader(in)) // 用于解压数据
	if err != nil {
		var out []byte
		return out
	}
	defer reader.Close()
	out, _ := ioutil.ReadAll(reader) // 读取解压后的数据
	return out
}

func UnPack() {
	for {
		taasReply, ok := <-UnPackCH // 从通道中获取replay
		if ok {
			UnGZipedReply := UGZipBytes([]byte(taasReply))     // 解压replay数据
			testMessage := &taas_proto.Message{}               // 存储解压后的数据
			err := proto.Unmarshal(UnGZipedReply, testMessage) // 反序列化
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
